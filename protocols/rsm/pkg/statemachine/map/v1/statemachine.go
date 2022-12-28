// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"bytes"
	"github.com/atomix/atomix/api/errors"
	mapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/map/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"sync"
)

const (
	Name       = "Map"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]) statemachine.PrimitiveStateMachine[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput] {
			return newExecutor(NewMapStateMachine(context))
		}, mapCodec)
}

type MapContext interface {
	statemachine.PrimitiveContext[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]
	Events() statemachine.Proposals[*mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]) MapContext {
	return &mapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput](context).
			Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.EventsInput, bool) {
				if events, ok := input.Input.(*mapprotocolv1.MapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *mapprotocolv1.EventsOutput) *mapprotocolv1.MapOutput {
				return &mapprotocolv1.MapOutput{
					Output: &mapprotocolv1.MapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type mapContext struct {
	statemachine.PrimitiveContext[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]
	events statemachine.Proposals[*mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput]
}

func (c *mapContext) Events() statemachine.Proposals[*mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput] {
	return c.events
}

type MapStateMachine interface {
	statemachine.Context[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]
	statemachine.Recoverable
	Put(proposal statemachine.Proposal[*mapprotocolv1.PutInput, *mapprotocolv1.PutOutput])
	Insert(proposal statemachine.Proposal[*mapprotocolv1.InsertInput, *mapprotocolv1.InsertOutput])
	Update(proposal statemachine.Proposal[*mapprotocolv1.UpdateInput, *mapprotocolv1.UpdateOutput])
	Remove(proposal statemachine.Proposal[*mapprotocolv1.RemoveInput, *mapprotocolv1.RemoveOutput])
	Clear(proposal statemachine.Proposal[*mapprotocolv1.ClearInput, *mapprotocolv1.ClearOutput])
	Events(proposal statemachine.Proposal[*mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput])
	Size(query statemachine.Query[*mapprotocolv1.SizeInput, *mapprotocolv1.SizeOutput])
	Get(query statemachine.Query[*mapprotocolv1.GetInput, *mapprotocolv1.GetOutput])
	Entries(query statemachine.Query[*mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput])
}

func NewMapStateMachine(context statemachine.PrimitiveContext[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]) MapStateMachine {
	return &mapStateMachine{
		MapContext: newContext(context),
		listeners:  make(map[statemachine.ProposalID]*mapprotocolv1.MapListener),
		entries:    make(map[string]*mapprotocolv1.MapEntry),
		timers:     make(map[string]statemachine.CancelFunc),
		watchers:   make(map[statemachine.QueryID]statemachine.Query[*mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput]),
	}
}

type mapStateMachine struct {
	MapContext
	listeners map[statemachine.ProposalID]*mapprotocolv1.MapListener
	entries   map[string]*mapprotocolv1.MapEntry
	timers    map[string]statemachine.CancelFunc
	watchers  map[statemachine.QueryID]statemachine.Query[*mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput]
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
		listener := &mapprotocolv1.MapListener{}
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
		entry := &mapprotocolv1.MapEntry{}
		if err := reader.ReadMessage(entry); err != nil {
			return err
		}
		s.entries[entry.Key] = entry
		s.scheduleTTL(entry.Key, entry)
	}
	return nil
}

func (s *mapStateMachine) Put(proposal statemachine.Proposal[*mapprotocolv1.PutInput, *mapprotocolv1.PutOutput]) {
	defer proposal.Close()

	oldEntry := s.entries[proposal.Input().Key]

	// If the value is equal to the current value, return a no-op.
	if oldEntry != nil && bytes.Equal(oldEntry.Value.Value, proposal.Input().Value) {
		proposal.Output(&mapprotocolv1.PutOutput{
			Index: oldEntry.Value.Index,
		})
		return
	}

	// Create a new entry and increment the revision number
	newEntry := &mapprotocolv1.MapEntry{
		Key: proposal.Input().Key,
		Value: &mapprotocolv1.MapValue{
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
		s.notify(newEntry, &mapprotocolv1.EventsOutput{
			Event: mapprotocolv1.Event{
				Key: newEntry.Key,
				Event: &mapprotocolv1.Event_Updated_{
					Updated: &mapprotocolv1.Event_Updated{
						Value: mapprotocolv1.IndexedValue{
							Value: newEntry.Value.Value,
							Index: newEntry.Value.Index,
						},
						PrevValue: mapprotocolv1.IndexedValue{
							Value: oldEntry.Value.Value,
							Index: oldEntry.Value.Index,
						},
					},
				},
			},
		})
		proposal.Output(&mapprotocolv1.PutOutput{
			Index: newEntry.Value.Index,
			PrevValue: &mapprotocolv1.IndexedValue{
				Value: oldEntry.Value.Value,
				Index: oldEntry.Value.Index,
			},
		})
	} else {
		s.notify(newEntry, &mapprotocolv1.EventsOutput{
			Event: mapprotocolv1.Event{
				Key: newEntry.Key,
				Event: &mapprotocolv1.Event_Inserted_{
					Inserted: &mapprotocolv1.Event_Inserted{
						Value: mapprotocolv1.IndexedValue{
							Value: newEntry.Value.Value,
							Index: newEntry.Value.Index,
						},
					},
				},
			},
		})
		proposal.Output(&mapprotocolv1.PutOutput{
			Index: newEntry.Value.Index,
		})
	}
}

func (s *mapStateMachine) Insert(proposal statemachine.Proposal[*mapprotocolv1.InsertInput, *mapprotocolv1.InsertOutput]) {
	defer proposal.Close()

	if _, ok := s.entries[proposal.Input().Key]; ok {
		proposal.Error(errors.NewAlreadyExists("key '%s' already exists", proposal.Input().Key))
		return
	}

	// Create a new entry and increment the revision number
	newEntry := &mapprotocolv1.MapEntry{
		Key: proposal.Input().Key,
		Value: &mapprotocolv1.MapValue{
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
	s.notify(newEntry, &mapprotocolv1.EventsOutput{
		Event: mapprotocolv1.Event{
			Key: newEntry.Key,
			Event: &mapprotocolv1.Event_Inserted_{
				Inserted: &mapprotocolv1.Event_Inserted{
					Value: mapprotocolv1.IndexedValue{
						Value: newEntry.Value.Value,
						Index: newEntry.Value.Index,
					},
				},
			},
		},
	})

	proposal.Output(&mapprotocolv1.InsertOutput{
		Index: newEntry.Value.Index,
	})
}

func (s *mapStateMachine) Update(proposal statemachine.Proposal[*mapprotocolv1.UpdateInput, *mapprotocolv1.UpdateOutput]) {
	defer proposal.Close()

	oldEntry, ok := s.entries[proposal.Input().Key]
	if !ok {
		proposal.Error(errors.NewNotFound("key '%s' not found", proposal.Input().Key))
		return
	}

	if proposal.Input().PrevIndex > 0 && oldEntry.Value.Index != proposal.Input().PrevIndex {
		proposal.Error(errors.NewConflict("entry index %d does not match update index %d", oldEntry.Value.Index, proposal.Input().PrevIndex))
		return
	}

	// Create a new entry and increment the revision number
	newEntry := &mapprotocolv1.MapEntry{
		Key: proposal.Input().Key,
		Value: &mapprotocolv1.MapValue{
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
	s.notify(newEntry, &mapprotocolv1.EventsOutput{
		Event: mapprotocolv1.Event{
			Key: newEntry.Key,
			Event: &mapprotocolv1.Event_Updated_{
				Updated: &mapprotocolv1.Event_Updated{
					Value: mapprotocolv1.IndexedValue{
						Value: newEntry.Value.Value,
						Index: newEntry.Value.Index,
					},
					PrevValue: mapprotocolv1.IndexedValue{
						Value: oldEntry.Value.Value,
						Index: oldEntry.Value.Index,
					},
				},
			},
		},
	})

	proposal.Output(&mapprotocolv1.UpdateOutput{
		Index: newEntry.Value.Index,
		PrevValue: mapprotocolv1.IndexedValue{
			Value: oldEntry.Value.Value,
			Index: oldEntry.Value.Index,
		},
	})
}

func (s *mapStateMachine) Remove(proposal statemachine.Proposal[*mapprotocolv1.RemoveInput, *mapprotocolv1.RemoveOutput]) {
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
	s.notify(entry, &mapprotocolv1.EventsOutput{
		Event: mapprotocolv1.Event{
			Key: entry.Key,
			Event: &mapprotocolv1.Event_Removed_{
				Removed: &mapprotocolv1.Event_Removed{
					Value: mapprotocolv1.IndexedValue{
						Value: entry.Value.Value,
						Index: entry.Value.Index,
					},
				},
			},
		},
	})

	proposal.Output(&mapprotocolv1.RemoveOutput{
		Value: mapprotocolv1.IndexedValue{
			Value: entry.Value.Value,
			Index: entry.Value.Index,
		},
	})
}

func (s *mapStateMachine) Clear(proposal statemachine.Proposal[*mapprotocolv1.ClearInput, *mapprotocolv1.ClearOutput]) {
	defer proposal.Close()
	for key, entry := range s.entries {
		s.notify(entry, &mapprotocolv1.EventsOutput{
			Event: mapprotocolv1.Event{
				Key: entry.Key,
				Event: &mapprotocolv1.Event_Removed_{
					Removed: &mapprotocolv1.Event_Removed{
						Value: mapprotocolv1.IndexedValue{
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
	proposal.Output(&mapprotocolv1.ClearOutput{})
}

func (s *mapStateMachine) Events(proposal statemachine.Proposal[*mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput]) {
	listener := &mapprotocolv1.MapListener{
		Key: proposal.Input().Key,
	}
	s.listeners[proposal.ID()] = listener
	proposal.Output(&mapprotocolv1.EventsOutput{
		Event: mapprotocolv1.Event{
			Key: proposal.Input().Key,
		},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *mapStateMachine) Size(query statemachine.Query[*mapprotocolv1.SizeInput, *mapprotocolv1.SizeOutput]) {
	defer query.Close()
	query.Output(&mapprotocolv1.SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *mapStateMachine) Get(query statemachine.Query[*mapprotocolv1.GetInput, *mapprotocolv1.GetOutput]) {
	defer query.Close()
	entry, ok := s.entries[query.Input().Key]
	if !ok {
		query.Error(errors.NewNotFound("key %s not found", query.Input().Key))
	} else {
		query.Output(&mapprotocolv1.GetOutput{
			Value: mapprotocolv1.IndexedValue{
				Value: entry.Value.Value,
				Index: entry.Value.Index,
			},
		})
	}
}

func (s *mapStateMachine) Entries(query statemachine.Query[*mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput]) {
	for _, entry := range s.entries {
		query.Output(&mapprotocolv1.EntriesOutput{
			Entry: mapprotocolv1.Entry{
				Key: entry.Key,
				Value: &mapprotocolv1.IndexedValue{
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

func (s *mapStateMachine) notify(entry *mapprotocolv1.MapEntry, event *mapprotocolv1.EventsOutput) {
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
			watcher.Output(&mapprotocolv1.EntriesOutput{
				Entry: mapprotocolv1.Entry{
					Key: entry.Key,
					Value: &mapprotocolv1.IndexedValue{
						Value: entry.Value.Value,
						Index: entry.Value.Index,
					},
				},
			})
		} else {
			watcher.Output(&mapprotocolv1.EntriesOutput{
				Entry: mapprotocolv1.Entry{
					Key: entry.Key,
				},
			})
		}
	}
}

func (s *mapStateMachine) scheduleTTL(key string, entry *mapprotocolv1.MapEntry) {
	s.cancelTTL(key)
	if entry.Value.Expire != nil {
		s.timers[key] = s.Scheduler().Schedule(*entry.Value.Expire, func() {
			delete(s.entries, key)
			s.notify(entry, &mapprotocolv1.EventsOutput{
				Event: mapprotocolv1.Event{
					Key: key,
					Event: &mapprotocolv1.Event_Removed_{
						Removed: &mapprotocolv1.Event_Removed{
							Value: mapprotocolv1.IndexedValue{
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
