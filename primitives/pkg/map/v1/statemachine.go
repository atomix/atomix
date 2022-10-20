// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"bytes"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"sync"
)

const Service = "atomix.runtime.map.v1.Map"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*MapInput, *MapOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*MapInput, *MapOutput](Service, stateMachineCodec,
	func(context statemachine.PrimitiveContext[*MapInput, *MapOutput]) statemachine.Executor[*MapInput, *MapOutput] {
		return newExecutor(NewMapStateMachine(context))
	})

type MapContext interface {
	statemachine.PrimitiveContext[*MapInput, *MapOutput]
	Events() statemachine.Proposals[*EventsInput, *EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*MapInput, *MapOutput]) MapContext {
	return &mapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*MapInput, *MapOutput, *EventsInput, *EventsOutput](context).
			Decoder(func(input *MapInput) (*EventsInput, bool) {
				if events, ok := input.Input.(*MapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *EventsOutput) *MapOutput {
				return &MapOutput{
					Output: &MapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type mapContext struct {
	statemachine.PrimitiveContext[*MapInput, *MapOutput]
	events statemachine.Proposals[*EventsInput, *EventsOutput]
}

func (c *mapContext) Events() statemachine.Proposals[*EventsInput, *EventsOutput] {
	return c.events
}

type MapStateMachine interface {
	statemachine.Context[*MapInput, *MapOutput]
	statemachine.Recoverable
	Put(proposal statemachine.Proposal[*PutInput, *PutOutput])
	Insert(proposal statemachine.Proposal[*InsertInput, *InsertOutput])
	Update(proposal statemachine.Proposal[*UpdateInput, *UpdateOutput])
	Remove(proposal statemachine.Proposal[*RemoveInput, *RemoveOutput])
	Clear(proposal statemachine.Proposal[*ClearInput, *ClearOutput])
	Events(proposal statemachine.Proposal[*EventsInput, *EventsOutput])
	Size(query statemachine.Query[*SizeInput, *SizeOutput])
	Get(query statemachine.Query[*GetInput, *GetOutput])
	Entries(query statemachine.Query[*EntriesInput, *EntriesOutput])
}

func NewMapStateMachine(context statemachine.PrimitiveContext[*MapInput, *MapOutput]) MapStateMachine {
	return &mapStateMachine{
		MapContext: newContext(context),
		listeners:  make(map[statemachine.ProposalID]*MapListener),
		entries:    make(map[string]*MapEntry),
		timers:     make(map[string]statemachine.CancelFunc),
		watchers:   make(map[statemachine.QueryID]statemachine.Query[*EntriesInput, *EntriesOutput]),
	}
}

type mapStateMachine struct {
	MapContext
	listeners map[statemachine.ProposalID]*MapListener
	entries   map[string]*MapEntry
	timers    map[string]statemachine.CancelFunc
	watchers  map[statemachine.QueryID]statemachine.Query[*EntriesInput, *EntriesOutput]
	mu        sync.RWMutex
}

func (s *mapStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	s.Log().Infow("Persisting Map to snapshot")
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
	for _, entry := range s.entries {
		if err := writer.WriteMessage(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *mapStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	s.Log().Infow("Recovering Map from snapshot")
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		proposal, ok := s.MapContext.Events().Get(statemachine.ProposalID(proposalID))
		if !ok {
			return errors.NewFault("cannot find proposal %d", proposalID)
		}
		listener := &MapListener{}
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
		entry := &MapEntry{}
		if err := reader.ReadMessage(entry); err != nil {
			return err
		}
		s.entries[entry.Key] = entry
		s.scheduleTTL(entry.Key, entry)
	}
	return nil
}

func (s *mapStateMachine) Put(proposal statemachine.Proposal[*PutInput, *PutOutput]) {
	defer proposal.Close()

	oldEntry := s.entries[proposal.Input().Key]

	// If the value is equal to the current value, return a no-op.
	if oldEntry != nil && bytes.Equal(oldEntry.Value.Value, proposal.Input().Value) {
		proposal.Output(&PutOutput{
			Index: oldEntry.Value.Index,
		})
		return
	}

	// Create a new entry and increment the revision number
	newEntry := &MapEntry{
		Key: proposal.Input().Key,
		Value: &MapValue{
			Value: proposal.Input().Value,
			Index: s.Index(),
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		newEntry.Value.Expire = &expire
	}

	// Create a new entry value and set it in the map.
	s.entries[proposal.Input().Key] = newEntry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(proposal.Input().Key, newEntry)

	// Publish an event to listener streams.
	if oldEntry != nil {
		s.notify(newEntry, &EventsOutput{
			Event: Event{
				Key: newEntry.Key,
				Event: &Event_Updated_{
					Updated: &Event_Updated{
						Value: IndexedValue{
							Value: newEntry.Value.Value,
							Index: newEntry.Value.Index,
						},
						PrevValue: IndexedValue{
							Value: oldEntry.Value.Value,
							Index: oldEntry.Value.Index,
						},
					},
				},
			},
		})
		proposal.Output(&PutOutput{
			Index: newEntry.Value.Index,
			PrevValue: &IndexedValue{
				Value: oldEntry.Value.Value,
				Index: oldEntry.Value.Index,
			},
		})
	} else {
		s.notify(newEntry, &EventsOutput{
			Event: Event{
				Key: newEntry.Key,
				Event: &Event_Inserted_{
					Inserted: &Event_Inserted{
						Value: IndexedValue{
							Value: newEntry.Value.Value,
							Index: newEntry.Value.Index,
						},
					},
				},
			},
		})
		proposal.Output(&PutOutput{
			Index: newEntry.Value.Index,
		})
	}
}

func (s *mapStateMachine) Insert(proposal statemachine.Proposal[*InsertInput, *InsertOutput]) {
	defer proposal.Close()

	if _, ok := s.entries[proposal.Input().Key]; ok {
		proposal.Error(errors.NewAlreadyExists("key '%s' already exists", proposal.Input().Key))
		return
	}

	// Create a new entry and increment the revision number
	newEntry := &MapEntry{
		Key: proposal.Input().Key,
		Value: &MapValue{
			Value: proposal.Input().Value,
			Index: s.Index(),
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		newEntry.Value.Expire = &expire
	}

	// Create a new entry value and set it in the map.
	s.entries[proposal.Input().Key] = newEntry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(proposal.Input().Key, newEntry)

	// Publish an event to listener streams.
	s.notify(newEntry, &EventsOutput{
		Event: Event{
			Key: newEntry.Key,
			Event: &Event_Inserted_{
				Inserted: &Event_Inserted{
					Value: IndexedValue{
						Value: newEntry.Value.Value,
						Index: newEntry.Value.Index,
					},
				},
			},
		},
	})

	proposal.Output(&InsertOutput{
		Index: newEntry.Value.Index,
	})
}

func (s *mapStateMachine) Update(proposal statemachine.Proposal[*UpdateInput, *UpdateOutput]) {
	defer proposal.Close()

	oldEntry, ok := s.entries[proposal.Input().Key]
	if !ok {
		proposal.Error(errors.NewNotFound("key '%s' not found", proposal.Input().Key))
		return
	}

	if proposal.Input().PrevIndex > 0 && oldEntry.Value.Index != proposal.Input().PrevIndex {
		proposal.Error(errors.NewConflict("entry index %d does not match remove index %d", oldEntry.Value.Index, proposal.Input().PrevIndex))
		return
	}

	// Create a new entry and increment the revision number
	newEntry := &MapEntry{
		Key: proposal.Input().Key,
		Value: &MapValue{
			Value: proposal.Input().Value,
			Index: s.Index(),
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		newEntry.Value.Expire = &expire
	}

	// Create a new entry value and set it in the map.
	s.entries[proposal.Input().Key] = newEntry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(proposal.Input().Key, newEntry)

	// Publish an event to listener streams.
	s.notify(newEntry, &EventsOutput{
		Event: Event{
			Key: newEntry.Key,
			Event: &Event_Updated_{
				Updated: &Event_Updated{
					Value: IndexedValue{
						Value: newEntry.Value.Value,
						Index: newEntry.Value.Index,
					},
					PrevValue: IndexedValue{
						Value: oldEntry.Value.Value,
						Index: oldEntry.Value.Index,
					},
				},
			},
		},
	})

	proposal.Output(&UpdateOutput{
		Index: newEntry.Value.Index,
		PrevValue: IndexedValue{
			Value: oldEntry.Value.Value,
			Index: oldEntry.Value.Index,
		},
	})
}

func (s *mapStateMachine) Remove(proposal statemachine.Proposal[*RemoveInput, *RemoveOutput]) {
	defer proposal.Close()
	entry, ok := s.entries[proposal.Input().Key]
	if !ok {
		proposal.Error(errors.NewNotFound("key '%s' not found", proposal.Input().Key))
		return
	}

	if proposal.Input().PrevIndex > 0 && entry.Value.Index != proposal.Input().PrevIndex {
		proposal.Error(errors.NewConflict("entry index %d does not match remove index %d", entry.Value.Index, proposal.Input().PrevIndex))
		return
	}

	delete(s.entries, proposal.Input().Key)

	// Schedule the timeout for the value if necessary.
	s.cancelTTL(entry.Key)

	// Publish an event to listener streams.
	s.notify(entry, &EventsOutput{
		Event: Event{
			Key: entry.Key,
			Event: &Event_Removed_{
				Removed: &Event_Removed{
					Value: IndexedValue{
						Value: entry.Value.Value,
						Index: entry.Value.Index,
					},
				},
			},
		},
	})

	proposal.Output(&RemoveOutput{
		Value: IndexedValue{
			Value: entry.Value.Value,
			Index: entry.Value.Index,
		},
	})
}

func (s *mapStateMachine) Clear(proposal statemachine.Proposal[*ClearInput, *ClearOutput]) {
	defer proposal.Close()
	for key, entry := range s.entries {
		s.notify(entry, &EventsOutput{
			Event: Event{
				Key: entry.Key,
				Event: &Event_Removed_{
					Removed: &Event_Removed{
						Value: IndexedValue{
							Value: entry.Value.Value,
							Index: entry.Value.Index,
						},
					},
				},
			},
		})
		s.cancelTTL(key)
		delete(s.entries, key)
	}
	proposal.Output(&ClearOutput{})
}

func (s *mapStateMachine) Events(proposal statemachine.Proposal[*EventsInput, *EventsOutput]) {
	listener := &MapListener{
		Key: proposal.Input().Key,
	}
	s.listeners[proposal.ID()] = listener
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *mapStateMachine) Size(query statemachine.Query[*SizeInput, *SizeOutput]) {
	defer query.Close()
	query.Output(&SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *mapStateMachine) Get(query statemachine.Query[*GetInput, *GetOutput]) {
	defer query.Close()
	entry, ok := s.entries[query.Input().Key]
	if !ok {
		query.Error(errors.NewNotFound("key %s not found", query.Input().Key))
	} else {
		query.Output(&GetOutput{
			Value: IndexedValue{
				Value: entry.Value.Value,
				Index: entry.Value.Index,
			},
		})
	}
}

func (s *mapStateMachine) Entries(query statemachine.Query[*EntriesInput, *EntriesOutput]) {
	for _, entry := range s.entries {
		query.Output(&EntriesOutput{
			Entry: Entry{
				Key: entry.Key,
				Value: &IndexedValue{
					Value: entry.Value.Value,
					Index: entry.Value.Index,
				},
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

func (s *mapStateMachine) notify(entry *MapEntry, event *EventsOutput) {
	for proposalID, listener := range s.listeners {
		if listener.Key == "" || listener.Key == event.Event.Key {
			proposal, ok := s.MapContext.Events().Get(proposalID)
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
		if entry.Value != nil {
			watcher.Output(&EntriesOutput{
				Entry: Entry{
					Key: entry.Key,
					Value: &IndexedValue{
						Value: entry.Value.Value,
						Index: entry.Value.Index,
					},
				},
			})
		} else {
			watcher.Output(&EntriesOutput{
				Entry: Entry{
					Key: entry.Key,
				},
			})
		}
	}
}

func (s *mapStateMachine) scheduleTTL(key string, entry *MapEntry) {
	s.cancelTTL(key)
	if entry.Value.Expire != nil {
		s.timers[key] = s.Scheduler().Schedule(*entry.Value.Expire, func() {
			delete(s.entries, key)
			s.notify(entry, &EventsOutput{
				Event: Event{
					Key: key,
					Event: &Event_Removed_{
						Removed: &Event_Removed{
							Value: IndexedValue{
								Value: entry.Value.Value,
								Index: entry.Value.Index,
							},
							Expired: true,
						},
					},
				},
			})
		})
	}
}

func (s *mapStateMachine) cancelTTL(key string) {
	ttlCancelFunc, ok := s.timers[key]
	if ok {
		ttlCancelFunc()
	}
}
