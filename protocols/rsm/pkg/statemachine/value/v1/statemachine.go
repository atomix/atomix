// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valueprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"sync"
)

const Service = "atomix.runtime.value.v1.Value"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput](Service, valueCodec,
	func(context statemachine.PrimitiveContext[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]) statemachine.Executor[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput] {
		return newExecutor(NewValueStateMachine(context))
	})

type ValueContext interface {
	statemachine.PrimitiveContext[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]
	Events() statemachine.Proposals[*valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]) ValueContext {
	return &valueContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput](context).
			Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.EventsInput, bool) {
				if events, ok := input.Input.(*valueprotocolv1.ValueInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *valueprotocolv1.EventsOutput) *valueprotocolv1.ValueOutput {
				return &valueprotocolv1.ValueOutput{
					Output: &valueprotocolv1.ValueOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type valueContext struct {
	statemachine.PrimitiveContext[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]
	events statemachine.Proposals[*valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput]
}

func (c *valueContext) Events() statemachine.Proposals[*valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput] {
	return c.events
}

type ValueStateMachine interface {
	statemachine.Context[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]
	statemachine.Recoverable
	Set(statemachine.Proposal[*valueprotocolv1.SetInput, *valueprotocolv1.SetOutput])
	Insert(statemachine.Proposal[*valueprotocolv1.InsertInput, *valueprotocolv1.InsertOutput])
	Update(statemachine.Proposal[*valueprotocolv1.UpdateInput, *valueprotocolv1.UpdateOutput])
	Delete(statemachine.Proposal[*valueprotocolv1.DeleteInput, *valueprotocolv1.DeleteOutput])
	Events(statemachine.Proposal[*valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput])
	Get(statemachine.Query[*valueprotocolv1.GetInput, *valueprotocolv1.GetOutput])
	Watch(statemachine.Query[*valueprotocolv1.WatchInput, *valueprotocolv1.WatchOutput])
}

func NewValueStateMachine(context statemachine.PrimitiveContext[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]) ValueStateMachine {
	return &valueStateMachine{
		ValueContext: newContext(context),
		listeners:    make(map[statemachine.ProposalID]bool),
		watchers:     make(map[statemachine.QueryID]statemachine.Query[*valueprotocolv1.WatchInput, *valueprotocolv1.WatchOutput]),
	}
}

type valueStateMachine struct {
	ValueContext
	value         *valueprotocolv1.ValueState
	listeners     map[statemachine.ProposalID]bool
	ttlCancelFunc statemachine.CancelFunc
	watchers      map[statemachine.QueryID]statemachine.Query[*valueprotocolv1.WatchInput, *valueprotocolv1.WatchOutput]
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

	state := &valueprotocolv1.ValueState{}
	if err := reader.ReadMessage(state); err != nil {
		return err
	}
	s.scheduleTTL(state)
	s.value = state
	return nil
}

func (s *valueStateMachine) Set(proposal statemachine.Proposal[*valueprotocolv1.SetInput, *valueprotocolv1.SetOutput]) {
	defer proposal.Close()

	oldValue := s.value
	newValue := &valueprotocolv1.ValueState{
		Value: &valueprotocolv1.IndexedValue{
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
		s.notify(newValue.Value, &valueprotocolv1.EventsOutput{
			Event: valueprotocolv1.Event{
				Event: &valueprotocolv1.Event_Created_{
					Created: &valueprotocolv1.Event_Created{
						Value: *newValue.Value,
					},
				},
			},
		})
		proposal.Output(&valueprotocolv1.SetOutput{
			Index: newValue.Value.Index,
		})
	} else {
		s.notify(newValue.Value, &valueprotocolv1.EventsOutput{
			Event: valueprotocolv1.Event{
				Event: &valueprotocolv1.Event_Updated_{
					Updated: &valueprotocolv1.Event_Updated{
						Value:     *newValue.Value,
						PrevValue: *oldValue.Value,
					},
				},
			},
		})
		proposal.Output(&valueprotocolv1.SetOutput{
			Index:     newValue.Value.Index,
			PrevValue: oldValue.Value,
		})
	}
}

func (s *valueStateMachine) Insert(proposal statemachine.Proposal[*valueprotocolv1.InsertInput, *valueprotocolv1.InsertOutput]) {
	defer proposal.Close()

	if s.value != nil {
		proposal.Error(errors.NewAlreadyExists("value already set"))
		return
	}

	newValue := &valueprotocolv1.ValueState{
		Value: &valueprotocolv1.IndexedValue{
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
	s.notify(newValue.Value, &valueprotocolv1.EventsOutput{
		Event: valueprotocolv1.Event{
			Event: &valueprotocolv1.Event_Created_{
				Created: &valueprotocolv1.Event_Created{
					Value: *newValue.Value,
				},
			},
		},
	})

	proposal.Output(&valueprotocolv1.InsertOutput{
		Index: newValue.Value.Index,
	})
}

func (s *valueStateMachine) Update(proposal statemachine.Proposal[*valueprotocolv1.UpdateInput, *valueprotocolv1.UpdateOutput]) {
	defer proposal.Close()

	if s.value == nil {
		proposal.Error(errors.NewNotFound("value not set"))
		return
	}

	if proposal.Input().PrevIndex > 0 && s.value.Value.Index != proposal.Input().PrevIndex {
		proposal.Error(errors.NewConflict("value index %d does not match update index %d", s.value.Value.Index, proposal.Input().PrevIndex))
	}

	oldValue := s.value
	newValue := &valueprotocolv1.ValueState{
		Value: &valueprotocolv1.IndexedValue{
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
	s.notify(newValue.Value, &valueprotocolv1.EventsOutput{
		Event: valueprotocolv1.Event{
			Event: &valueprotocolv1.Event_Updated_{
				Updated: &valueprotocolv1.Event_Updated{
					Value:     *newValue.Value,
					PrevValue: *oldValue.Value,
				},
			},
		},
	})

	proposal.Output(&valueprotocolv1.UpdateOutput{
		Index:     newValue.Value.Index,
		PrevValue: *oldValue.Value,
	})
}

func (s *valueStateMachine) Delete(proposal statemachine.Proposal[*valueprotocolv1.DeleteInput, *valueprotocolv1.DeleteOutput]) {
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
	s.notify(value.Value, &valueprotocolv1.EventsOutput{
		Event: valueprotocolv1.Event{
			Event: &valueprotocolv1.Event_Deleted_{
				Deleted: &valueprotocolv1.Event_Deleted{
					Value: *value.Value,
				},
			},
		},
	})

	proposal.Output(&valueprotocolv1.DeleteOutput{
		Value: *value.Value,
	})
}

func (s *valueStateMachine) Events(proposal statemachine.Proposal[*valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput]) {
	s.listeners[proposal.ID()] = true
	proposal.Output(&valueprotocolv1.EventsOutput{
		Event: valueprotocolv1.Event{},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *valueStateMachine) Get(query statemachine.Query[*valueprotocolv1.GetInput, *valueprotocolv1.GetOutput]) {
	defer query.Close()
	if s.value == nil {
		query.Error(errors.NewNotFound("value not set"))
	} else {
		query.Output(&valueprotocolv1.GetOutput{
			Value: s.value.Value,
		})
	}
}

func (s *valueStateMachine) Watch(query statemachine.Query[*valueprotocolv1.WatchInput, *valueprotocolv1.WatchOutput]) {
	if s.value != nil {
		query.Output(&valueprotocolv1.WatchOutput{
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

func (s *valueStateMachine) notify(value *valueprotocolv1.IndexedValue, event *valueprotocolv1.EventsOutput) {
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
		watcher.Output(&valueprotocolv1.WatchOutput{
			Value: value,
		})
	}
}

func (s *valueStateMachine) scheduleTTL(state *valueprotocolv1.ValueState) {
	s.cancelTTL()
	if state.Expire != nil {
		s.ttlCancelFunc = s.Scheduler().Schedule(*state.Expire, func() {
			s.value = nil
			s.notify(state.Value, &valueprotocolv1.EventsOutput{
				Event: valueprotocolv1.Event{
					Event: &valueprotocolv1.Event_Deleted_{
						Deleted: &valueprotocolv1.Event_Deleted{
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
