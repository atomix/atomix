// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/errors"
	setprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/set/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"sync"
)

const (
	Name       = "Set"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*setprotocolv1.SetInput, *setprotocolv1.SetOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]) statemachine.PrimitiveStateMachine[*setprotocolv1.SetInput, *setprotocolv1.SetOutput] {
			return newExecutor(NewSetStateMachine(context))
		}, setCodec)
}

type SetContext interface {
	statemachine.PrimitiveContext[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]
	Events() statemachine.Proposals[*setprotocolv1.EventsInput, *setprotocolv1.EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]) SetContext {
	return &setContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.EventsInput, *setprotocolv1.EventsOutput](context).
			Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.EventsInput, bool) {
				if events, ok := input.Input.(*setprotocolv1.SetInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *setprotocolv1.EventsOutput) *setprotocolv1.SetOutput {
				return &setprotocolv1.SetOutput{
					Output: &setprotocolv1.SetOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type setContext struct {
	statemachine.PrimitiveContext[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]
	events statemachine.Proposals[*setprotocolv1.EventsInput, *setprotocolv1.EventsOutput]
}

func (c *setContext) Events() statemachine.Proposals[*setprotocolv1.EventsInput, *setprotocolv1.EventsOutput] {
	return c.events
}

type SetStateMachine interface {
	statemachine.Context[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]
	statemachine.Recoverable
	Add(statemachine.Proposal[*setprotocolv1.AddInput, *setprotocolv1.AddOutput])
	Remove(statemachine.Proposal[*setprotocolv1.RemoveInput, *setprotocolv1.RemoveOutput])
	Clear(statemachine.Proposal[*setprotocolv1.ClearInput, *setprotocolv1.ClearOutput])
	Events(statemachine.Proposal[*setprotocolv1.EventsInput, *setprotocolv1.EventsOutput])
	Size(statemachine.Query[*setprotocolv1.SizeInput, *setprotocolv1.SizeOutput])
	Contains(statemachine.Query[*setprotocolv1.ContainsInput, *setprotocolv1.ContainsOutput])
	Elements(statemachine.Query[*setprotocolv1.ElementsInput, *setprotocolv1.ElementsOutput])
}

func NewSetStateMachine(context statemachine.PrimitiveContext[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]) SetStateMachine {
	return &setStateMachine{
		SetContext: newContext(context),
		listeners:  make(map[statemachine.ProposalID]bool),
		entries:    make(map[string]*setprotocolv1.SetElement),
		timers:     make(map[string]statemachine.CancelFunc),
		watchers:   make(map[statemachine.QueryID]statemachine.Query[*setprotocolv1.ElementsInput, *setprotocolv1.ElementsOutput]),
	}
}

type setStateMachine struct {
	SetContext
	listeners map[statemachine.ProposalID]bool
	entries   map[string]*setprotocolv1.SetElement
	timers    map[string]statemachine.CancelFunc
	watchers  map[statemachine.QueryID]statemachine.Query[*setprotocolv1.ElementsInput, *setprotocolv1.ElementsOutput]
	mu        sync.RWMutex
}

func (s *setStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	if err := writer.WriteVarInt(len(s.listeners)); err != nil {
		return err
	}
	for proposalID := range s.listeners {
		if err := writer.WriteVarUint64(uint64(proposalID)); err != nil {
			return err
		}
	}

	if err := writer.WriteVarInt(len(s.entries)); err != nil {
		return err
	}
	for key, entry := range s.entries {
		if err := writer.WriteString(key); err != nil {
			return err
		}
		if err := writer.WriteMessage(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *setStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		proposal, ok := s.SetContext.Events().Get(statemachine.ProposalID(proposalID))
		if !ok {
			return errors.NewFault("cannot find proposal %d", proposalID)
		}
		s.listeners[proposal.ID()] = true
		proposal.Watch(func(state statemachine.ProposalState) {
			if state != statemachine.Running {
				delete(s.listeners, proposal.ID())
			}
		})
	}

	n, err = reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := reader.ReadString()
		if err != nil {
			return err
		}
		element := &setprotocolv1.SetElement{}
		if err := reader.ReadMessage(element); err != nil {
			return err
		}
		s.entries[key] = element
		s.scheduleTTL(key, element)
	}
	return nil
}

func (s *setStateMachine) Add(proposal statemachine.Proposal[*setprotocolv1.AddInput, *setprotocolv1.AddOutput]) {
	defer proposal.Close()

	value := proposal.Input().Element.Value
	if _, ok := s.entries[value]; ok {
		proposal.Output(&setprotocolv1.AddOutput{
			Added: false,
		})
		return
	}

	element := &setprotocolv1.SetElement{}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		element.Expire = &expire
	}

	// Create a new entry value and set it in the set.
	s.entries[value] = element

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(value, element)

	s.notify(value, &setprotocolv1.EventsOutput{
		Event: setprotocolv1.Event{
			Event: &setprotocolv1.Event_Added_{
				Added: &setprotocolv1.Event_Added{
					Element: setprotocolv1.Element{
						Value: value,
					},
				},
			},
		},
	})
	proposal.Output(&setprotocolv1.AddOutput{
		Added: true,
	})
}

func (s *setStateMachine) Remove(proposal statemachine.Proposal[*setprotocolv1.RemoveInput, *setprotocolv1.RemoveOutput]) {
	defer proposal.Close()

	value := proposal.Input().Element.Value
	if _, ok := s.entries[value]; !ok {
		proposal.Output(&setprotocolv1.RemoveOutput{
			Removed: false,
		})
		return
	}

	s.cancelTTL(value)
	delete(s.entries, value)

	s.notify(value, &setprotocolv1.EventsOutput{
		Event: setprotocolv1.Event{
			Event: &setprotocolv1.Event_Removed_{
				Removed: &setprotocolv1.Event_Removed{
					Element: setprotocolv1.Element{
						Value: value,
					},
				},
			},
		},
	})
	proposal.Output(&setprotocolv1.RemoveOutput{
		Removed: true,
	})
}

func (s *setStateMachine) Clear(proposal statemachine.Proposal[*setprotocolv1.ClearInput, *setprotocolv1.ClearOutput]) {
	defer proposal.Close()
	for value := range s.entries {
		s.notify(value, &setprotocolv1.EventsOutput{
			Event: setprotocolv1.Event{
				Event: &setprotocolv1.Event_Removed_{
					Removed: &setprotocolv1.Event_Removed{
						Element: setprotocolv1.Element{
							Value: value,
						},
					},
				},
			},
		})
		s.cancelTTL(value)
		delete(s.entries, value)
	}
	proposal.Output(&setprotocolv1.ClearOutput{})
}

func (s *setStateMachine) Events(proposal statemachine.Proposal[*setprotocolv1.EventsInput, *setprotocolv1.EventsOutput]) {
	s.listeners[proposal.ID()] = true
	proposal.Output(&setprotocolv1.EventsOutput{
		Event: setprotocolv1.Event{},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *setStateMachine) Size(query statemachine.Query[*setprotocolv1.SizeInput, *setprotocolv1.SizeOutput]) {
	defer query.Close()
	query.Output(&setprotocolv1.SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *setStateMachine) Contains(query statemachine.Query[*setprotocolv1.ContainsInput, *setprotocolv1.ContainsOutput]) {
	defer query.Close()
	if _, ok := s.entries[query.Input().Element.Value]; ok {
		query.Output(&setprotocolv1.ContainsOutput{
			Contains: true,
		})
	} else {
		query.Output(&setprotocolv1.ContainsOutput{
			Contains: false,
		})
	}
}

func (s *setStateMachine) Elements(query statemachine.Query[*setprotocolv1.ElementsInput, *setprotocolv1.ElementsOutput]) {
	for value := range s.entries {
		query.Output(&setprotocolv1.ElementsOutput{
			Element: setprotocolv1.Element{
				Value: value,
			},
		})
	}

	if query.Input().Watch {
		s.mu.Lock()
		s.watchers[query.ID()] = query
		s.mu.Unlock()
		query.Watch(func(state statemachine.QueryState) {
			if state != statemachine.Running {
				s.mu.Lock()
				delete(s.watchers, query.ID())
				s.mu.Unlock()
			}
		})
	} else {
		query.Close()
	}
}

func (s *setStateMachine) notify(value string, event *setprotocolv1.EventsOutput) {
	for proposalID := range s.listeners {
		proposal, ok := s.SetContext.Events().Get(proposalID)
		if ok {
			proposal.Output(event)
		} else {
			delete(s.listeners, proposalID)
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&setprotocolv1.ElementsOutput{
			Element: setprotocolv1.Element{
				Value: value,
			},
		})
	}
}

func (s *setStateMachine) scheduleTTL(value string, element *setprotocolv1.SetElement) {
	s.cancelTTL(value)
	if element.Expire != nil {
		s.timers[value] = s.Scheduler().Schedule(*element.Expire, func() {
			delete(s.entries, value)
			s.notify(value, &setprotocolv1.EventsOutput{
				Event: setprotocolv1.Event{
					Event: &setprotocolv1.Event_Removed_{
						Removed: &setprotocolv1.Event_Removed{
							Element: setprotocolv1.Element{
								Value: value,
							},
							Expired: true,
						},
					},
				},
			})
		})
	}
}

func (s *setStateMachine) cancelTTL(key string) {
	ttlCancelFunc, ok := s.timers[key]
	if ok {
		ttlCancelFunc()
	}
}
