// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"sync"
)

const Service = "atomix.runtime.countermap.v1.CounterMap"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*CounterMapInput, *CounterMapOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*CounterMapInput, *CounterMapOutput](Service, counterMapCodec,
	func(context statemachine.PrimitiveContext[*CounterMapInput, *CounterMapOutput]) statemachine.Executor[*CounterMapInput, *CounterMapOutput] {
		return newExecutor(NewCounterMapStateMachine(context))
	})

type CounterMapContext interface {
	statemachine.PrimitiveContext[*CounterMapInput, *CounterMapOutput]
	Events() statemachine.Proposals[*EventsInput, *EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*CounterMapInput, *CounterMapOutput]) CounterMapContext {
	return &counterMapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*CounterMapInput, *CounterMapOutput, *EventsInput, *EventsOutput](context).
			Decoder(func(input *CounterMapInput) (*EventsInput, bool) {
				if events, ok := input.Input.(*CounterMapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *EventsOutput) *CounterMapOutput {
				return &CounterMapOutput{
					Output: &CounterMapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type counterMapContext struct {
	statemachine.PrimitiveContext[*CounterMapInput, *CounterMapOutput]
	events statemachine.Proposals[*EventsInput, *EventsOutput]
}

func (c *counterMapContext) Events() statemachine.Proposals[*EventsInput, *EventsOutput] {
	return c.events
}

type CounterMapStateMachine interface {
	statemachine.Context[*CounterMapInput, *CounterMapOutput]
	statemachine.Recoverable
	Set(statemachine.Proposal[*SetInput, *SetOutput])
	Insert(statemachine.Proposal[*InsertInput, *InsertOutput])
	Update(statemachine.Proposal[*UpdateInput, *UpdateOutput])
	Remove(statemachine.Proposal[*RemoveInput, *RemoveOutput])
	Increment(statemachine.Proposal[*IncrementInput, *IncrementOutput])
	Decrement(statemachine.Proposal[*DecrementInput, *DecrementOutput])
	Clear(statemachine.Proposal[*ClearInput, *ClearOutput])
	Events(statemachine.Proposal[*EventsInput, *EventsOutput])
	Size(statemachine.Query[*SizeInput, *SizeOutput])
	Get(statemachine.Query[*GetInput, *GetOutput])
	Entries(statemachine.Query[*EntriesInput, *EntriesOutput])
}

func NewCounterMapStateMachine(context statemachine.PrimitiveContext[*CounterMapInput, *CounterMapOutput]) CounterMapStateMachine {
	return &counterMapStateMachine{
		CounterMapContext: newContext(context),
		listeners:         make(map[statemachine.ProposalID]*CounterMapListener),
		entries:           make(map[string]int64),
		watchers:          make(map[statemachine.QueryID]statemachine.Query[*EntriesInput, *EntriesOutput]),
	}
}

type counterMapStateMachine struct {
	CounterMapContext
	listeners map[statemachine.ProposalID]*CounterMapListener
	entries   map[string]int64
	watchers  map[statemachine.QueryID]statemachine.Query[*EntriesInput, *EntriesOutput]
	mu        sync.RWMutex
}

func (s *counterMapStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	s.Log().Infow("Persisting CounterMap to snapshot")
	if err := writer.WriteVarInt(len(s.listeners)); err != nil {
		return err
	}
	for proposalID, listener := range s.listeners {
		if err := writer.WriteVarUint64(uint64(proposalID)); err != nil {
			return err
		}
		if err := writer.WriteMessage(listener); err != nil {
			return err
		}
	}

	if err := writer.WriteVarInt(len(s.entries)); err != nil {
		return err
	}
	for key, value := range s.entries {
		if err := writer.WriteString(key); err != nil {
			return err
		}
		if err := writer.WriteVarInt64(value); err != nil {
			return err
		}
	}
	return nil
}

func (s *counterMapStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	s.Log().Infow("Recovering CounterMap from snapshot")
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		proposal, ok := s.CounterMapContext.Events().Get(statemachine.ProposalID(proposalID))
		if !ok {
			return errors.NewFault("cannot find proposal %d", proposalID)
		}
		listener := &CounterMapListener{}
		if err := reader.ReadMessage(listener); err != nil {
			return err
		}
		s.listeners[proposal.ID()] = listener
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
		value, err := reader.ReadVarInt64()
		if err != nil {
			return err
		}
		s.entries[key] = value
	}
	return nil
}

func (s *counterMapStateMachine) Set(proposal statemachine.Proposal[*SetInput, *SetOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	newValue := proposal.Input().Value
	oldValue, updated := s.entries[key]
	s.entries[key] = newValue

	// Publish an event to listener streams.
	if updated {
		s.notify(key, newValue, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Updated_{
					Updated: &Event_Updated{
						Value:     newValue,
						PrevValue: oldValue,
					},
				},
			},
		})
		proposal.Output(&SetOutput{
			PrevValue: oldValue,
		})
	} else {
		s.notify(key, newValue, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Inserted_{
					Inserted: &Event_Inserted{
						Value: newValue,
					},
				},
			},
		})
		proposal.Output(&SetOutput{
			PrevValue: oldValue,
		})
	}
}

func (s *counterMapStateMachine) Insert(proposal statemachine.Proposal[*InsertInput, *InsertOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	value := proposal.Input().Value

	if _, ok := s.entries[key]; ok {
		proposal.Error(errors.NewAlreadyExists("key '%s' already exists", key))
		return
	}

	s.entries[key] = value

	s.notify(key, value, &EventsOutput{
		Event: Event{
			Key: key,
			Event: &Event_Inserted_{
				Inserted: &Event_Inserted{
					Value: value,
				},
			},
		},
	})
	proposal.Output(&InsertOutput{})
}

func (s *counterMapStateMachine) Update(proposal statemachine.Proposal[*UpdateInput, *UpdateOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	newValue := proposal.Input().Value

	oldValue, updated := s.entries[key]
	if !updated {
		proposal.Error(errors.NewNotFound("key '%s' not found", key))
		return
	}

	if oldValue != proposal.Input().PrevValue {
		proposal.Error(errors.NewConflict("optimistic lock failure"))
		return
	}

	s.entries[key] = newValue

	// Publish an event to listener streams.
	s.notify(key, newValue, &EventsOutput{
		Event: Event{
			Key: key,
			Event: &Event_Updated_{
				Updated: &Event_Updated{
					Value:     newValue,
					PrevValue: oldValue,
				},
			},
		},
	})
	proposal.Output(&UpdateOutput{
		PrevValue: oldValue,
	})
}

func (s *counterMapStateMachine) Increment(proposal statemachine.Proposal[*IncrementInput, *IncrementOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	delta := proposal.Input().Delta
	if delta == 0 {
		delta = 1
	}

	oldValue, updated := s.entries[key]
	newValue := oldValue + delta
	s.entries[key] = newValue

	// Publish an event to listener streams.
	if updated {
		s.notify(key, delta, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Updated_{
					Updated: &Event_Updated{
						Value:     newValue,
						PrevValue: oldValue,
					},
				},
			},
		})
	} else {
		s.notify(key, delta, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Inserted_{
					Inserted: &Event_Inserted{
						Value: newValue,
					},
				},
			},
		})
	}
	proposal.Output(&IncrementOutput{
		PrevValue: oldValue,
	})
}

func (s *counterMapStateMachine) Decrement(proposal statemachine.Proposal[*DecrementInput, *DecrementOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	delta := proposal.Input().Delta
	if delta == 0 {
		delta = 1
	}

	oldValue, updated := s.entries[key]
	newValue := oldValue - delta
	s.entries[key] = newValue

	// Publish an event to listener streams.
	if updated {
		s.notify(key, delta, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Updated_{
					Updated: &Event_Updated{
						Value:     newValue,
						PrevValue: oldValue,
					},
				},
			},
		})
	} else {
		s.notify(key, delta, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Inserted_{
					Inserted: &Event_Inserted{
						Value: newValue,
					},
				},
			},
		})
	}
	proposal.Output(&DecrementOutput{
		PrevValue: oldValue,
	})
}

func (s *counterMapStateMachine) Remove(proposal statemachine.Proposal[*RemoveInput, *RemoveOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	value, ok := s.entries[key]
	if !ok {
		proposal.Error(errors.NewNotFound("key '%s' not found", key))
		return
	}

	if proposal.Input().PrevValue != value {
		proposal.Error(errors.NewConflict("entry value %d does not match remove value %d", value, proposal.Input().PrevValue))
		return
	}

	delete(s.entries, key)

	// Publish an event to listener streams.
	s.notify(key, value, &EventsOutput{
		Event: Event{
			Key: key,
			Event: &Event_Removed_{
				Removed: &Event_Removed{
					Value: value,
				},
			},
		},
	})
	proposal.Output(&RemoveOutput{
		Value: value,
	})
}

func (s *counterMapStateMachine) Clear(proposal statemachine.Proposal[*ClearInput, *ClearOutput]) {
	defer proposal.Close()
	for key, value := range s.entries {
		s.notify(key, value, &EventsOutput{
			Event: Event{
				Key: key,
				Event: &Event_Removed_{
					Removed: &Event_Removed{
						Value: value,
					},
				},
			},
		})
		delete(s.entries, key)
	}
	proposal.Output(&ClearOutput{})
}

func (s *counterMapStateMachine) Events(proposal statemachine.Proposal[*EventsInput, *EventsOutput]) {
	listener := &CounterMapListener{
		Key: proposal.Input().Key,
	}
	s.listeners[proposal.ID()] = listener
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *counterMapStateMachine) Size(query statemachine.Query[*SizeInput, *SizeOutput]) {
	defer query.Close()
	query.Output(&SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *counterMapStateMachine) Get(query statemachine.Query[*GetInput, *GetOutput]) {
	defer query.Close()
	value, ok := s.entries[query.Input().Key]
	if !ok {
		query.Error(errors.NewNotFound("key %s not found", query.Input().Key))
	} else {
		query.Output(&GetOutput{
			Value: value,
		})
	}
}

func (s *counterMapStateMachine) Entries(query statemachine.Query[*EntriesInput, *EntriesOutput]) {
	for key, value := range s.entries {
		query.Output(&EntriesOutput{
			Entry: Entry{
				Key:   key,
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

func (s *counterMapStateMachine) notify(key string, value int64, event *EventsOutput) {
	for proposalID, listener := range s.listeners {
		if listener.Key == "" || listener.Key == event.Event.Key {
			proposal, ok := s.CounterMapContext.Events().Get(proposalID)
			if ok {
				proposal.Output(event)
			} else {
				delete(s.listeners, proposalID)
			}
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&EntriesOutput{
			Entry: Entry{
				Key:   key,
				Value: value,
			},
		})
	}
}
