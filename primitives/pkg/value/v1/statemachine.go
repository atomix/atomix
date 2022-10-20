// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"sync"
)

const Service = "atomix.runtime.value.v1.Value"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*ValueInput, *ValueOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*ValueInput, *ValueOutput](Service, valueCodec,
	func(context statemachine.PrimitiveContext[*ValueInput, *ValueOutput]) statemachine.Executor[*ValueInput, *ValueOutput] {
		return newExecutor(NewValueStateMachine(context))
	})

type ValueContext interface {
	statemachine.PrimitiveContext[*ValueInput, *ValueOutput]
	Events() statemachine.Proposals[*EventsInput, *EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*ValueInput, *ValueOutput]) ValueContext {
	return &valueContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*ValueInput, *ValueOutput, *EventsInput, *EventsOutput](context).
			Decoder(func(input *ValueInput) (*EventsInput, bool) {
				if events, ok := input.Input.(*ValueInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *EventsOutput) *ValueOutput {
				return &ValueOutput{
					Output: &ValueOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type valueContext struct {
	statemachine.PrimitiveContext[*ValueInput, *ValueOutput]
	events statemachine.Proposals[*EventsInput, *EventsOutput]
}

func (c *valueContext) Events() statemachine.Proposals[*EventsInput, *EventsOutput] {
	return c.events
}

type ValueStateMachine interface {
	statemachine.Context[*ValueInput, *ValueOutput]
	statemachine.Recoverable
	Set(statemachine.Proposal[*SetInput, *SetOutput])
	Insert(statemachine.Proposal[*InsertInput, *InsertOutput])
	Update(statemachine.Proposal[*UpdateInput, *UpdateOutput])
	Delete(statemachine.Proposal[*DeleteInput, *DeleteOutput])
	Events(statemachine.Proposal[*EventsInput, *EventsOutput])
	Get(statemachine.Query[*GetInput, *GetOutput])
	Watch(statemachine.Query[*WatchInput, *WatchOutput])
}

func NewValueStateMachine(context statemachine.PrimitiveContext[*ValueInput, *ValueOutput]) ValueStateMachine {
	return &valueStateMachine{
		ValueContext: newContext(context),
		listeners:    make(map[statemachine.ProposalID]bool),
		watchers:     make(map[statemachine.QueryID]statemachine.Query[*WatchInput, *WatchOutput]),
	}
}

type valueStateMachine struct {
	ValueContext
	value         *ValueState
	listeners     map[statemachine.ProposalID]bool
	ttlCancelFunc statemachine.CancelFunc
	watchers      map[statemachine.QueryID]statemachine.Query[*WatchInput, *WatchOutput]
	mu            sync.RWMutex
}

func (s *valueStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	s.Log().Infow("Persisting Value to snapshot")
	if err := writer.WriteVarInt(len(s.listeners)); err != nil {
		return err
	}
	for proposalID := range s.listeners {
		if err := writer.WriteVarUint64(uint64(proposalID)); err != nil {
			return err
		}
	}
	if s.value == nil {
		if err := writer.WriteBool(false); err != nil {
			return err
		}
	} else {
		if err := writer.WriteBool(true); err != nil {
			return err
		}
		if err := writer.WriteMessage(s.value); err != nil {
			return err
		}
	}
	return nil
}

func (s *valueStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	s.Log().Infow("Recovering Value from snapshot")
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		proposal, ok := s.ValueContext.Events().Get(statemachine.ProposalID(proposalID))
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

	exists, err := reader.ReadBool()
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	state := &ValueState{}
	if err := reader.ReadMessage(state); err != nil {
		return err
	}
	s.scheduleTTL(state)
	s.value = state
	return nil
}

func (s *valueStateMachine) Set(proposal statemachine.Proposal[*SetInput, *SetOutput]) {
	defer proposal.Close()

	oldValue := s.value
	newValue := &ValueState{
		Value: &IndexedValue{
			Value: proposal.Input().Value,
			Index: s.Index(),
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		newValue.Expire = &expire
	}

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(newValue)
	s.value = newValue

	// Publish an event to listener streams.
	if oldValue == nil {
		s.notify(newValue.Value, &EventsOutput{
			Event: Event{
				Event: &Event_Created_{
					Created: &Event_Created{
						Value: *newValue.Value,
					},
				},
			},
		})
		proposal.Output(&SetOutput{
			Index: newValue.Value.Index,
		})
	} else {
		s.notify(newValue.Value, &EventsOutput{
			Event: Event{
				Event: &Event_Updated_{
					Updated: &Event_Updated{
						Value:     *newValue.Value,
						PrevValue: *oldValue.Value,
					},
				},
			},
		})
		proposal.Output(&SetOutput{
			Index:     newValue.Value.Index,
			PrevValue: oldValue.Value,
		})
	}
}

func (s *valueStateMachine) Insert(proposal statemachine.Proposal[*InsertInput, *InsertOutput]) {
	defer proposal.Close()

	if s.value != nil {
		proposal.Error(errors.NewAlreadyExists("value already set"))
		return
	}

	newValue := &ValueState{
		Value: &IndexedValue{
			Value: proposal.Input().Value,
			Index: s.Index(),
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		newValue.Expire = &expire
	}

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(newValue)
	s.value = newValue

	// Publish an event to listener streams.
	s.notify(newValue.Value, &EventsOutput{
		Event: Event{
			Event: &Event_Created_{
				Created: &Event_Created{
					Value: *newValue.Value,
				},
			},
		},
	})

	proposal.Output(&InsertOutput{
		Index: newValue.Value.Index,
	})
}

func (s *valueStateMachine) Update(proposal statemachine.Proposal[*UpdateInput, *UpdateOutput]) {
	defer proposal.Close()

	if s.value == nil {
		proposal.Error(errors.NewNotFound("value not set"))
		return
	}

	if proposal.Input().PrevIndex > 0 && s.value.Value.Index != proposal.Input().PrevIndex {
		proposal.Error(errors.NewConflict("value index %d does not match update index %d", s.value.Value.Index, proposal.Input().PrevIndex))
	}

	oldValue := s.value
	newValue := &ValueState{
		Value: &IndexedValue{
			Value: proposal.Input().Value,
			Index: s.Index(),
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		newValue.Expire = &expire
	}

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(newValue)
	s.value = newValue

	// Publish an event to listener streams.
	s.notify(newValue.Value, &EventsOutput{
		Event: Event{
			Event: &Event_Updated_{
				Updated: &Event_Updated{
					Value:     *newValue.Value,
					PrevValue: *oldValue.Value,
				},
			},
		},
	})

	proposal.Output(&UpdateOutput{
		Index:     newValue.Value.Index,
		PrevValue: *oldValue.Value,
	})
}

func (s *valueStateMachine) Delete(proposal statemachine.Proposal[*DeleteInput, *DeleteOutput]) {
	defer proposal.Close()

	if s.value == nil {
		proposal.Error(errors.NewNotFound("value not set"))
		return
	}

	if proposal.Input().PrevIndex > 0 && s.value.Value.Index != proposal.Input().PrevIndex {
		proposal.Error(errors.NewConflict("value index %d does not match delete index %d", s.value.Value.Index, proposal.Input().PrevIndex))
	}

	value := s.value
	s.cancelTTL()
	s.value = nil

	// Publish an event to listener streams.
	s.notify(value.Value, &EventsOutput{
		Event: Event{
			Event: &Event_Deleted_{
				Deleted: &Event_Deleted{
					Value: *value.Value,
				},
			},
		},
	})

	proposal.Output(&DeleteOutput{
		Value: *value.Value,
	})
}

func (s *valueStateMachine) Events(proposal statemachine.Proposal[*EventsInput, *EventsOutput]) {
	s.listeners[proposal.ID()] = true
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *valueStateMachine) Get(query statemachine.Query[*GetInput, *GetOutput]) {
	defer query.Close()
	if s.value == nil {
		query.Error(errors.NewNotFound("value not set"))
	} else {
		query.Output(&GetOutput{
			Value: s.value.Value,
		})
	}
}

func (s *valueStateMachine) Watch(query statemachine.Query[*WatchInput, *WatchOutput]) {
	if s.value != nil {
		query.Output(&WatchOutput{
			Value: s.value.Value,
		})
	}

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
}

func (s *valueStateMachine) notify(value *IndexedValue, event *EventsOutput) {
	for proposalID := range s.listeners {
		proposal, ok := s.ValueContext.Events().Get(proposalID)
		if ok {
			proposal.Output(event)
		} else {
			delete(s.listeners, proposalID)
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&WatchOutput{
			Value: value,
		})
	}
}

func (s *valueStateMachine) scheduleTTL(state *ValueState) {
	s.cancelTTL()
	if state.Expire != nil {
		s.ttlCancelFunc = s.Scheduler().Schedule(*state.Expire, func() {
			s.value = nil
			s.notify(state.Value, &EventsOutput{
				Event: Event{
					Event: &Event_Deleted_{
						Deleted: &Event_Deleted{
							Value:   *state.Value,
							Expired: true,
						},
					},
				},
			})
		})
	}
}

func (s *valueStateMachine) cancelTTL() {
	if s.ttlCancelFunc != nil {
		s.ttlCancelFunc()
		s.ttlCancelFunc = nil
	}
}
