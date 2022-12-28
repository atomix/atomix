// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/errors"
	countermapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/countermap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"sync"
)

const (
	Name       = "CounterMap"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]) statemachine.PrimitiveStateMachine[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput] {
			return newExecutor(NewCounterMapStateMachine(context))
		}, counterMapCodec)
}

type CounterMapContext interface {
	statemachine.PrimitiveContext[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]
	Events() statemachine.Proposals[*countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]) CounterMapContext {
	return &counterMapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput](context).
			Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.EventsInput, bool) {
				if events, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *countermapprotocolv1.EventsOutput) *countermapprotocolv1.CounterMapOutput {
				return &countermapprotocolv1.CounterMapOutput{
					Output: &countermapprotocolv1.CounterMapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type counterMapContext struct {
	statemachine.PrimitiveContext[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]
	events statemachine.Proposals[*countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput]
}

func (c *counterMapContext) Events() statemachine.Proposals[*countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput] {
	return c.events
}

type CounterMapStateMachine interface {
	statemachine.Context[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]
	statemachine.Recoverable
	Set(statemachine.Proposal[*countermapprotocolv1.SetInput, *countermapprotocolv1.SetOutput])
	Insert(statemachine.Proposal[*countermapprotocolv1.InsertInput, *countermapprotocolv1.InsertOutput])
	Update(statemachine.Proposal[*countermapprotocolv1.UpdateInput, *countermapprotocolv1.UpdateOutput])
	Remove(statemachine.Proposal[*countermapprotocolv1.RemoveInput, *countermapprotocolv1.RemoveOutput])
	Increment(statemachine.Proposal[*countermapprotocolv1.IncrementInput, *countermapprotocolv1.IncrementOutput])
	Decrement(statemachine.Proposal[*countermapprotocolv1.DecrementInput, *countermapprotocolv1.DecrementOutput])
	Clear(statemachine.Proposal[*countermapprotocolv1.ClearInput, *countermapprotocolv1.ClearOutput])
	Events(statemachine.Proposal[*countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput])
	Size(statemachine.Query[*countermapprotocolv1.SizeInput, *countermapprotocolv1.SizeOutput])
	Get(statemachine.Query[*countermapprotocolv1.GetInput, *countermapprotocolv1.GetOutput])
	Entries(statemachine.Query[*countermapprotocolv1.EntriesInput, *countermapprotocolv1.EntriesOutput])
}

func NewCounterMapStateMachine(context statemachine.PrimitiveContext[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]) CounterMapStateMachine {
	return &counterMapStateMachine{
		CounterMapContext: newContext(context),
		listeners:         make(map[statemachine.ProposalID]*countermapprotocolv1.CounterMapListener),
		entries:           make(map[string]int64),
		watchers:          make(map[statemachine.QueryID]statemachine.Query[*countermapprotocolv1.EntriesInput, *countermapprotocolv1.EntriesOutput]),
	}
}

type counterMapStateMachine struct {
	CounterMapContext
	listeners map[statemachine.ProposalID]*countermapprotocolv1.CounterMapListener
	entries   map[string]int64
	watchers  map[statemachine.QueryID]statemachine.Query[*countermapprotocolv1.EntriesInput, *countermapprotocolv1.EntriesOutput]
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
		listener := &countermapprotocolv1.CounterMapListener{}
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

func (s *counterMapStateMachine) Set(proposal statemachine.Proposal[*countermapprotocolv1.SetInput, *countermapprotocolv1.SetOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	newValue := proposal.Input().Value
	oldValue, updated := s.entries[key]
	s.entries[key] = newValue

	// Publish an event to listener streams.
	if updated {
		s.notify(key, newValue, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Updated_{
					Updated: &countermapprotocolv1.Event_Updated{
						Value:     newValue,
						PrevValue: oldValue,
					},
				},
			},
		})
		proposal.Output(&countermapprotocolv1.SetOutput{
			PrevValue: oldValue,
		})
	} else {
		s.notify(key, newValue, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Inserted_{
					Inserted: &countermapprotocolv1.Event_Inserted{
						Value: newValue,
					},
				},
			},
		})
		proposal.Output(&countermapprotocolv1.SetOutput{
			PrevValue: oldValue,
		})
	}
}

func (s *counterMapStateMachine) Insert(proposal statemachine.Proposal[*countermapprotocolv1.InsertInput, *countermapprotocolv1.InsertOutput]) {
	defer proposal.Close()

	key := proposal.Input().Key
	value := proposal.Input().Value

	if _, ok := s.entries[key]; ok {
		proposal.Error(errors.NewAlreadyExists("key '%s' already exists", key))
		return
	}

	s.entries[key] = value

	s.notify(key, value, &countermapprotocolv1.EventsOutput{
		Event: countermapprotocolv1.Event{
			Key: key,
			Event: &countermapprotocolv1.Event_Inserted_{
				Inserted: &countermapprotocolv1.Event_Inserted{
					Value: value,
				},
			},
		},
	})
	proposal.Output(&countermapprotocolv1.InsertOutput{})
}

func (s *counterMapStateMachine) Update(proposal statemachine.Proposal[*countermapprotocolv1.UpdateInput, *countermapprotocolv1.UpdateOutput]) {
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
	s.notify(key, newValue, &countermapprotocolv1.EventsOutput{
		Event: countermapprotocolv1.Event{
			Key: key,
			Event: &countermapprotocolv1.Event_Updated_{
				Updated: &countermapprotocolv1.Event_Updated{
					Value:     newValue,
					PrevValue: oldValue,
				},
			},
		},
	})
	proposal.Output(&countermapprotocolv1.UpdateOutput{
		PrevValue: oldValue,
	})
}

func (s *counterMapStateMachine) Increment(proposal statemachine.Proposal[*countermapprotocolv1.IncrementInput, *countermapprotocolv1.IncrementOutput]) {
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
		s.notify(key, delta, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Updated_{
					Updated: &countermapprotocolv1.Event_Updated{
						Value:     newValue,
						PrevValue: oldValue,
					},
				},
			},
		})
	} else {
		s.notify(key, delta, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Inserted_{
					Inserted: &countermapprotocolv1.Event_Inserted{
						Value: newValue,
					},
				},
			},
		})
	}
	proposal.Output(&countermapprotocolv1.IncrementOutput{
		PrevValue: oldValue,
	})
}

func (s *counterMapStateMachine) Decrement(proposal statemachine.Proposal[*countermapprotocolv1.DecrementInput, *countermapprotocolv1.DecrementOutput]) {
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
		s.notify(key, delta, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Updated_{
					Updated: &countermapprotocolv1.Event_Updated{
						Value:     newValue,
						PrevValue: oldValue,
					},
				},
			},
		})
	} else {
		s.notify(key, delta, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Inserted_{
					Inserted: &countermapprotocolv1.Event_Inserted{
						Value: newValue,
					},
				},
			},
		})
	}
	proposal.Output(&countermapprotocolv1.DecrementOutput{
		PrevValue: oldValue,
	})
}

func (s *counterMapStateMachine) Remove(proposal statemachine.Proposal[*countermapprotocolv1.RemoveInput, *countermapprotocolv1.RemoveOutput]) {
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
	s.notify(key, value, &countermapprotocolv1.EventsOutput{
		Event: countermapprotocolv1.Event{
			Key: key,
			Event: &countermapprotocolv1.Event_Removed_{
				Removed: &countermapprotocolv1.Event_Removed{
					Value: value,
				},
			},
		},
	})
	proposal.Output(&countermapprotocolv1.RemoveOutput{
		Value: value,
	})
}

func (s *counterMapStateMachine) Clear(proposal statemachine.Proposal[*countermapprotocolv1.ClearInput, *countermapprotocolv1.ClearOutput]) {
	defer proposal.Close()
	for key, value := range s.entries {
		s.notify(key, value, &countermapprotocolv1.EventsOutput{
			Event: countermapprotocolv1.Event{
				Key: key,
				Event: &countermapprotocolv1.Event_Removed_{
					Removed: &countermapprotocolv1.Event_Removed{
						Value: value,
					},
				},
			},
		})
		delete(s.entries, key)
	}
	proposal.Output(&countermapprotocolv1.ClearOutput{})
}

func (s *counterMapStateMachine) Events(proposal statemachine.Proposal[*countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput]) {
	listener := &countermapprotocolv1.CounterMapListener{
		Key: proposal.Input().Key,
	}
	s.listeners[proposal.ID()] = listener
	proposal.Output(&countermapprotocolv1.EventsOutput{
		Event: countermapprotocolv1.Event{
			Key: proposal.Input().Key,
		},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *counterMapStateMachine) Size(query statemachine.Query[*countermapprotocolv1.SizeInput, *countermapprotocolv1.SizeOutput]) {
	defer query.Close()
	query.Output(&countermapprotocolv1.SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *counterMapStateMachine) Get(query statemachine.Query[*countermapprotocolv1.GetInput, *countermapprotocolv1.GetOutput]) {
	defer query.Close()
	value, ok := s.entries[query.Input().Key]
	if !ok {
		query.Error(errors.NewNotFound("key %s not found", query.Input().Key))
	} else {
		query.Output(&countermapprotocolv1.GetOutput{
			Value: value,
		})
	}
}

func (s *counterMapStateMachine) Entries(query statemachine.Query[*countermapprotocolv1.EntriesInput, *countermapprotocolv1.EntriesOutput]) {
	for key, value := range s.entries {
		query.Output(&countermapprotocolv1.EntriesOutput{
			Entry: countermapprotocolv1.Entry{
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

func (s *counterMapStateMachine) notify(key string, value int64, event *countermapprotocolv1.EventsOutput) {
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
		watcher.Output(&countermapprotocolv1.EntriesOutput{
			Entry: countermapprotocolv1.Entry{
				Key:   key,
				Value: value,
			},
		})
	}
}
