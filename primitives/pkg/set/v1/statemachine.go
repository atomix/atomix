// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"sync"
)

const Service = "atomix.runtime.set.v1.Set"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*SetInput, *SetOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*SetInput, *SetOutput](Service, setCodec,
	func(context statemachine.PrimitiveContext[*SetInput, *SetOutput]) statemachine.Executor[*SetInput, *SetOutput] {
		return newExecutor(NewSetStateMachine(context))
	})

type SetContext interface {
	statemachine.PrimitiveContext[*SetInput, *SetOutput]
	Events() statemachine.Proposals[*EventsInput, *EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*SetInput, *SetOutput]) SetContext {
	return &setContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*SetInput, *SetOutput, *EventsInput, *EventsOutput](context).
			Decoder(func(input *SetInput) (*EventsInput, bool) {
				if events, ok := input.Input.(*SetInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *EventsOutput) *SetOutput {
				return &SetOutput{
					Output: &SetOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type setContext struct {
	statemachine.PrimitiveContext[*SetInput, *SetOutput]
	events statemachine.Proposals[*EventsInput, *EventsOutput]
}

func (c *setContext) Events() statemachine.Proposals[*EventsInput, *EventsOutput] {
	return c.events
}

type SetStateMachine interface {
	statemachine.Context[*SetInput, *SetOutput]
	statemachine.Recoverable
	Add(statemachine.Proposal[*AddInput, *AddOutput])
	Remove(statemachine.Proposal[*RemoveInput, *RemoveOutput])
	Clear(statemachine.Proposal[*ClearInput, *ClearOutput])
	Events(statemachine.Proposal[*EventsInput, *EventsOutput])
	Size(statemachine.Query[*SizeInput, *SizeOutput])
	Contains(statemachine.Query[*ContainsInput, *ContainsOutput])
	Elements(statemachine.Query[*ElementsInput, *ElementsOutput])
}

func NewSetStateMachine(context statemachine.PrimitiveContext[*SetInput, *SetOutput]) SetStateMachine {
	return &setStateMachine{
		SetContext: newContext(context),
		listeners:  make(map[statemachine.ProposalID]bool),
		entries:    make(map[string]*SetElement),
		timers:     make(map[string]statemachine.CancelFunc),
		watchers:   make(map[statemachine.QueryID]statemachine.Query[*ElementsInput, *ElementsOutput]),
	}
}

type setStateMachine struct {
	SetContext
	listeners map[statemachine.ProposalID]bool
	entries   map[string]*SetElement
	timers    map[string]statemachine.CancelFunc
	watchers  map[statemachine.QueryID]statemachine.Query[*ElementsInput, *ElementsOutput]
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
	for _, entry := range s.entries {
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
		element := &SetElement{}
		if err := reader.ReadMessage(element); err != nil {
			return err
		}
		s.entries[key] = element
		s.scheduleTTL(key, element)
	}
	return nil
}

func (s *setStateMachine) Add(proposal statemachine.Proposal[*AddInput, *AddOutput]) {
	defer proposal.Close()

	value := proposal.Input().Element.Value
	if _, ok := s.entries[value]; ok {
		proposal.Error(errors.NewAlreadyExists("value already exists in set"))
		return
	}

	element := &SetElement{}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		element.Expire = &expire
	}

	// Create a new entry value and set it in the set.
	s.entries[value] = element

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(value, element)

	s.notify(value, &EventsOutput{
		Event: Event{
			Event: &Event_Added_{
				Added: &Event_Added{
					Element: Element{
						Value: value,
					},
				},
			},
		},
	})
	proposal.Output(&AddOutput{})
}

func (s *setStateMachine) Remove(proposal statemachine.Proposal[*RemoveInput, *RemoveOutput]) {
	defer proposal.Close()

	value := proposal.Input().Element.Value
	if _, ok := s.entries[value]; !ok {
		proposal.Error(errors.NewNotFound("value not found in set"))
		return
	}

	s.cancelTTL(value)
	delete(s.entries, value)

	s.notify(value, &EventsOutput{
		Event: Event{
			Event: &Event_Removed_{
				Removed: &Event_Removed{
					Element: Element{
						Value: value,
					},
				},
			},
		},
	})
	proposal.Output(&RemoveOutput{})
}

func (s *setStateMachine) Clear(proposal statemachine.Proposal[*ClearInput, *ClearOutput]) {
	defer proposal.Close()
	for value := range s.entries {
		s.notify(value, &EventsOutput{
			Event: Event{
				Event: &Event_Removed_{
					Removed: &Event_Removed{
						Element: Element{
							Value: value,
						},
					},
				},
			},
		})
		s.cancelTTL(value)
		delete(s.entries, value)
	}
	proposal.Output(&ClearOutput{})
}

func (s *setStateMachine) Events(proposal statemachine.Proposal[*EventsInput, *EventsOutput]) {
	s.listeners[proposal.ID()] = true
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *setStateMachine) Size(query statemachine.Query[*SizeInput, *SizeOutput]) {
	defer query.Close()
	query.Output(&SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *setStateMachine) Contains(query statemachine.Query[*ContainsInput, *ContainsOutput]) {
	defer query.Close()
	if _, ok := s.entries[query.Input().Element.Value]; ok {
		query.Output(&ContainsOutput{
			Contains: true,
		})
	} else {
		query.Output(&ContainsOutput{
			Contains: false,
		})
	}
}

func (s *setStateMachine) Elements(query statemachine.Query[*ElementsInput, *ElementsOutput]) {
	for value := range s.entries {
		query.Output(&ElementsOutput{
			Element: Element{
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

func (s *setStateMachine) notify(value string, event *EventsOutput) {
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
		watcher.Output(&ElementsOutput{
			Element: Element{
				Value: value,
			},
		})
	}
}

func (s *setStateMachine) scheduleTTL(value string, element *SetElement) {
	s.cancelTTL(value)
	if element.Expire != nil {
		s.timers[value] = s.Scheduler().Schedule(*element.Expire, func() {
			delete(s.entries, value)
			s.notify(value, &EventsOutput{
				Event: Event{
					Event: &Event_Removed_{
						Removed: &Event_Removed{
							Element: Element{
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
