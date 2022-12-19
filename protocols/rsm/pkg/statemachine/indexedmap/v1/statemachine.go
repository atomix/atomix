// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"bytes"
	indexedmapprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/indexedmap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"sync"
)

const (
	Name       = "IndexedMap"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]) statemachine.PrimitiveStateMachine[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput] {
			return newExecutor(NewIndexedMapStateMachine(context))
		}, indexedMapCodec)
}

type IndexedMapContext interface {
	statemachine.PrimitiveContext[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]
	Events() statemachine.Proposals[*indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]) IndexedMapContext {
	return &indexedMapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput](context).
			Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.EventsInput, bool) {
				if events, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *indexedmapprotocolv1.EventsOutput) *indexedmapprotocolv1.IndexedMapOutput {
				return &indexedmapprotocolv1.IndexedMapOutput{
					Output: &indexedmapprotocolv1.IndexedMapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type indexedMapContext struct {
	statemachine.PrimitiveContext[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]
	events statemachine.Proposals[*indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput]
}

func (c *indexedMapContext) Events() statemachine.Proposals[*indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput] {
	return c.events
}

type IndexedMapStateMachine interface {
	statemachine.Context[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]
	statemachine.Recoverable
	Append(statemachine.Proposal[*indexedmapprotocolv1.AppendInput, *indexedmapprotocolv1.AppendOutput])
	Update(statemachine.Proposal[*indexedmapprotocolv1.UpdateInput, *indexedmapprotocolv1.UpdateOutput])
	Remove(statemachine.Proposal[*indexedmapprotocolv1.RemoveInput, *indexedmapprotocolv1.RemoveOutput])
	Clear(statemachine.Proposal[*indexedmapprotocolv1.ClearInput, *indexedmapprotocolv1.ClearOutput])
	Events(statemachine.Proposal[*indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput])
	Size(statemachine.Query[*indexedmapprotocolv1.SizeInput, *indexedmapprotocolv1.SizeOutput])
	Get(statemachine.Query[*indexedmapprotocolv1.GetInput, *indexedmapprotocolv1.GetOutput])
	FirstEntry(statemachine.Query[*indexedmapprotocolv1.FirstEntryInput, *indexedmapprotocolv1.FirstEntryOutput])
	LastEntry(statemachine.Query[*indexedmapprotocolv1.LastEntryInput, *indexedmapprotocolv1.LastEntryOutput])
	NextEntry(statemachine.Query[*indexedmapprotocolv1.NextEntryInput, *indexedmapprotocolv1.NextEntryOutput])
	PrevEntry(statemachine.Query[*indexedmapprotocolv1.PrevEntryInput, *indexedmapprotocolv1.PrevEntryOutput])
	Entries(statemachine.Query[*indexedmapprotocolv1.EntriesInput, *indexedmapprotocolv1.EntriesOutput])
}

func NewIndexedMapStateMachine(context statemachine.PrimitiveContext[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]) IndexedMapStateMachine {
	return &indexedMapStateMachine{
		IndexedMapContext: newContext(context),
		keys:              make(map[string]*LinkedMapEntryValue),
		indexes:           make(map[uint64]*LinkedMapEntryValue),
		streams:           make(map[statemachine.ProposalID]*indexedmapprotocolv1.IndexedMapListener),
		timers:            make(map[string]statemachine.CancelFunc),
		watchers:          make(map[statemachine.QueryID]statemachine.Query[*indexedmapprotocolv1.EntriesInput, *indexedmapprotocolv1.EntriesOutput]),
	}
}

// LinkedMapEntryValue is a doubly linked MapEntryValue
type LinkedMapEntryValue struct {
	*indexedmapprotocolv1.IndexedMapEntry
	Prev *LinkedMapEntryValue
	Next *LinkedMapEntryValue
}

type indexedMapStateMachine struct {
	IndexedMapContext
	lastIndex  uint64
	keys       map[string]*LinkedMapEntryValue
	indexes    map[uint64]*LinkedMapEntryValue
	firstEntry *LinkedMapEntryValue
	lastEntry  *LinkedMapEntryValue
	streams    map[statemachine.ProposalID]*indexedmapprotocolv1.IndexedMapListener
	timers     map[string]statemachine.CancelFunc
	watchers   map[statemachine.QueryID]statemachine.Query[*indexedmapprotocolv1.EntriesInput, *indexedmapprotocolv1.EntriesOutput]
	mu         sync.RWMutex
}

func (s *indexedMapStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	s.Log().Infow("Persisting IndexedMap to snapshot")
	if err := s.snapshotEntries(writer); err != nil {
		return err
	}
	if err := s.snapshotStreams(writer); err != nil {
		return err
	}
	return nil
}

func (s *indexedMapStateMachine) snapshotEntries(writer *statemachine.SnapshotWriter) error {
	if err := writer.WriteVarInt(len(s.keys)); err != nil {
		return err
	}
	entry := s.firstEntry
	for entry != nil {
		if err := writer.WriteMessage(s.firstEntry); err != nil {
			return err
		}
		entry = entry.Next
	}
	if err := writer.WriteVarUint64(s.lastIndex); err != nil {
		return err
	}
	return nil
}

func (s *indexedMapStateMachine) snapshotStreams(writer *statemachine.SnapshotWriter) error {
	if err := writer.WriteVarInt(len(s.streams)); err != nil {
		return err
	}
	for proposalID, listener := range s.streams {
		if err := writer.WriteVarUint64(uint64(proposalID)); err != nil {
			return err
		}
		if err := writer.WriteMessage(listener); err != nil {
			return err
		}
	}
	return nil
}

func (s *indexedMapStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	s.Log().Infow("Recovering IndexedMap from snapshot")
	if err := s.recoverEntries(reader); err != nil {
		return err
	}
	if err := s.recoverStreams(reader); err != nil {
		return err
	}
	return nil
}

func (s *indexedMapStateMachine) recoverEntries(reader *statemachine.SnapshotReader) error {
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}

	var prevEntry *LinkedMapEntryValue
	for i := 0; i < n; i++ {
		entry := &LinkedMapEntryValue{}
		if err := reader.ReadMessage(entry); err != nil {
			return err
		}
		s.keys[entry.Key] = entry
		s.indexes[entry.Index] = entry
		if s.firstEntry == nil {
			s.firstEntry = entry
		}
		if prevEntry != nil {
			prevEntry.Next = entry
			entry.Prev = prevEntry
		}
		prevEntry = entry
		s.lastEntry = entry
		s.scheduleTTL(entry)
	}

	i, err := reader.ReadVarUint64()
	if err != nil {
		return err
	}
	s.lastIndex = i
	return nil
}

func (s *indexedMapStateMachine) recoverStreams(reader *statemachine.SnapshotReader) error {
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		proposal, ok := s.IndexedMapContext.Events().Get(statemachine.ProposalID(proposalID))
		if !ok {
			return errors.NewFault("cannot find proposal %d", proposalID)
		}
		listener := &indexedmapprotocolv1.IndexedMapListener{}
		if err := reader.ReadMessage(listener); err != nil {
			return err
		}
		s.streams[proposal.ID()] = listener
		proposal.Watch(func(state statemachine.ProposalState) {
			if state != statemachine.Running {
				delete(s.streams, proposal.ID())
			}
		})
	}
	return nil
}

func (s *indexedMapStateMachine) Append(proposal statemachine.Proposal[*indexedmapprotocolv1.AppendInput, *indexedmapprotocolv1.AppendOutput]) {
	defer proposal.Close()

	// Check that the key does not already exist in the map
	if entry, ok := s.keys[proposal.Input().Key]; ok {
		proposal.Error(errors.NewAlreadyExists("key %s already exists at index %d", proposal.Input().Key, entry.Index))
		return
	}

	// Increment the map index
	s.lastIndex++
	index := s.lastIndex

	// Create a new entry value and set it in the map.
	entry := &LinkedMapEntryValue{
		IndexedMapEntry: &indexedmapprotocolv1.IndexedMapEntry{
			Index: index,
			Key:   proposal.Input().Key,
			Value: &indexedmapprotocolv1.IndexedMapValue{
				Value:   proposal.Input().Value,
				Version: uint64(proposal.ID()),
			},
		},
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		entry.Value.Expire = &expire
	}
	s.keys[entry.Key] = entry
	s.indexes[entry.Index] = entry

	// Set the first entry if not set
	if s.firstEntry == nil {
		s.firstEntry = entry
	}

	// If the last entry is set, link it to the new entry
	if s.lastEntry != nil {
		s.lastEntry.Next = entry
		entry.Prev = s.lastEntry
	}

	// Update the last entry
	s.lastEntry = entry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(entry)

	s.notify(entry, &indexedmapprotocolv1.Event{
		Key:   entry.Key,
		Index: entry.Index,
		Event: &indexedmapprotocolv1.Event_Inserted_{
			Inserted: &indexedmapprotocolv1.Event_Inserted{
				Value: *newStateMachineValue(entry.Value),
			},
		},
	})

	proposal.Output(&indexedmapprotocolv1.AppendOutput{
		Entry: newStateMachineEntry(entry.IndexedMapEntry),
	})
}

func (s *indexedMapStateMachine) Update(proposal statemachine.Proposal[*indexedmapprotocolv1.UpdateInput, *indexedmapprotocolv1.UpdateOutput]) {
	defer proposal.Close()

	// Get the current entry value by key, index, or both
	var oldEntry *LinkedMapEntryValue
	if proposal.Input().Index != 0 {
		if e, ok := s.indexes[proposal.Input().Index]; !ok {
			proposal.Error(errors.NewNotFound("index %d not found", proposal.Input().Index))
			return
		} else if proposal.Input().Key != "" && e.Key != proposal.Input().Key {
			proposal.Error(errors.NewFault("key at index %d does not match proposed Update key %s", proposal.Input().Index, proposal.Input().Key))
			return
		} else {
			oldEntry = e
		}
	} else if proposal.Input().Key != "" {
		if e, ok := s.keys[proposal.Input().Key]; !ok {
			proposal.Error(errors.NewNotFound("key %s not found", proposal.Input().Key))
			return
		} else {
			oldEntry = e
		}
	} else {
		proposal.Error(errors.NewInvalid("must specify either a key or index to update"))
		return
	}

	// If a prev_version was specified, check that the previous version matches
	if proposal.Input().PrevVersion != 0 && oldEntry.Value.Version != proposal.Input().PrevVersion {
		proposal.Error(errors.NewConflict("key %s version %d does not match prev_version %d", oldEntry.Key, oldEntry.Value.Version, proposal.Input().PrevVersion))
		return
	}

	// If the value is equal to the current value, return a no-op.
	if bytes.Equal(oldEntry.Value.Value, proposal.Input().Value) {
		proposal.Output(&indexedmapprotocolv1.UpdateOutput{
			Entry: newStateMachineEntry(oldEntry.IndexedMapEntry),
		})
		return
	}

	// Create a new entry value and set it in the map.
	entry := &LinkedMapEntryValue{
		IndexedMapEntry: &indexedmapprotocolv1.IndexedMapEntry{
			Index: oldEntry.Index,
			Key:   oldEntry.Key,
			Value: &indexedmapprotocolv1.IndexedMapValue{
				Value:   proposal.Input().Value,
				Version: uint64(proposal.ID()),
			},
		},
		Prev: oldEntry.Prev,
		Next: oldEntry.Next,
	}
	if proposal.Input().TTL != nil {
		expire := s.Scheduler().Time().Add(*proposal.Input().TTL)
		entry.Value.Expire = &expire
	}
	s.keys[entry.Key] = entry
	s.indexes[entry.Index] = entry

	// Update links for previous and next entries
	if oldEntry.Prev != nil {
		oldEntry.Prev.Next = entry
	} else {
		s.firstEntry = entry
	}
	if oldEntry.Next != nil {
		oldEntry.Next.Prev = entry
	} else {
		s.lastEntry = entry
	}

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(entry)

	s.notify(entry, &indexedmapprotocolv1.Event{
		Key:   entry.Key,
		Index: entry.Index,
		Event: &indexedmapprotocolv1.Event_Updated_{
			Updated: &indexedmapprotocolv1.Event_Updated{
				Value: *newStateMachineValue(entry.Value),
			},
		},
	})

	proposal.Output(&indexedmapprotocolv1.UpdateOutput{
		Entry: newStateMachineEntry(entry.IndexedMapEntry),
	})
}

func (s *indexedMapStateMachine) Remove(proposal statemachine.Proposal[*indexedmapprotocolv1.RemoveInput, *indexedmapprotocolv1.RemoveOutput]) {
	defer proposal.Close()

	var entry *LinkedMapEntryValue
	var ok bool
	if proposal.Input().Index != 0 {
		if entry, ok = s.indexes[proposal.Input().Index]; !ok {
			proposal.Error(errors.NewNotFound("no entry found at index %d", proposal.Input().Index))
			return
		}
	} else {
		if entry, ok = s.keys[proposal.Input().Key]; !ok {
			proposal.Error(errors.NewNotFound("no entry found at key %s", proposal.Input().Key))
			return
		}
	}

	if proposal.Input().PrevVersion != 0 && entry.Value.Version != proposal.Input().PrevVersion {
		proposal.Error(errors.NewConflict("key %s version %d does not match prev_version %d", entry.Key, entry.Value.Version, proposal.Input().PrevVersion))
		return
	}

	// Delete the entry from the map.
	delete(s.keys, entry.Key)
	delete(s.indexes, entry.Index)

	// Cancel any TTLs.
	s.cancelTTL(proposal.Input().Key)

	// Update links for previous and next entries
	if entry.Prev != nil {
		entry.Prev.Next = entry.Next
	} else {
		s.firstEntry = entry.Next
	}
	if entry.Next != nil {
		entry.Next.Prev = entry.Prev
	} else {
		s.lastEntry = entry.Prev
	}

	s.notify(entry, &indexedmapprotocolv1.Event{
		Key:   entry.Key,
		Index: entry.Index,
		Event: &indexedmapprotocolv1.Event_Removed_{
			Removed: &indexedmapprotocolv1.Event_Removed{
				Value: *newStateMachineValue(entry.IndexedMapEntry.Value),
			},
		},
	})

	proposal.Output(&indexedmapprotocolv1.RemoveOutput{
		Entry: newStateMachineEntry(entry.IndexedMapEntry),
	})
}

func (s *indexedMapStateMachine) Clear(proposal statemachine.Proposal[*indexedmapprotocolv1.ClearInput, *indexedmapprotocolv1.ClearOutput]) {
	defer proposal.Close()
	for key, entry := range s.keys {
		s.notify(entry, &indexedmapprotocolv1.Event{
			Key:   entry.Key,
			Index: entry.Index,
			Event: &indexedmapprotocolv1.Event_Removed_{
				Removed: &indexedmapprotocolv1.Event_Removed{
					Value: *newStateMachineValue(entry.IndexedMapEntry.Value),
				},
			},
		})
		s.cancelTTL(key)
	}
	s.keys = make(map[string]*LinkedMapEntryValue)
	s.indexes = make(map[uint64]*LinkedMapEntryValue)
	s.firstEntry = nil
	s.lastEntry = nil
	proposal.Output(&indexedmapprotocolv1.ClearOutput{})
}

func (s *indexedMapStateMachine) Events(proposal statemachine.Proposal[*indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput]) {
	listener := &indexedmapprotocolv1.IndexedMapListener{
		Key: proposal.Input().Key,
	}
	s.streams[proposal.ID()] = listener
	proposal.Output(&indexedmapprotocolv1.EventsOutput{
		Event: indexedmapprotocolv1.Event{
			Key: proposal.Input().Key,
		},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.streams, proposal.ID())
		}
	})
}

func (s *indexedMapStateMachine) Size(query statemachine.Query[*indexedmapprotocolv1.SizeInput, *indexedmapprotocolv1.SizeOutput]) {
	defer query.Close()
	query.Output(&indexedmapprotocolv1.SizeOutput{
		Size_: uint32(len(s.keys)),
	})
}

func (s *indexedMapStateMachine) Get(query statemachine.Query[*indexedmapprotocolv1.GetInput, *indexedmapprotocolv1.GetOutput]) {
	defer query.Close()

	var entry *LinkedMapEntryValue
	var ok bool
	if query.Input().Index > 0 {
		if entry, ok = s.indexes[query.Input().Index]; !ok {
			query.Error(errors.NewNotFound("no entry found at index %d", query.Input().Index))
			return
		}
	} else {
		if entry, ok = s.keys[query.Input().Key]; !ok {
			query.Error(errors.NewNotFound("no entry found at key %s", query.Input().Key))
			return
		}
	}

	query.Output(&indexedmapprotocolv1.GetOutput{
		Entry: newStateMachineEntry(entry.IndexedMapEntry),
	})
}

func (s *indexedMapStateMachine) FirstEntry(query statemachine.Query[*indexedmapprotocolv1.FirstEntryInput, *indexedmapprotocolv1.FirstEntryOutput]) {
	defer query.Close()
	if s.firstEntry == nil {
		query.Error(errors.NewNotFound("map is empty"))
	} else {
		query.Output(&indexedmapprotocolv1.FirstEntryOutput{
			Entry: newStateMachineEntry(s.firstEntry.IndexedMapEntry),
		})
	}
}

func (s *indexedMapStateMachine) LastEntry(query statemachine.Query[*indexedmapprotocolv1.LastEntryInput, *indexedmapprotocolv1.LastEntryOutput]) {
	defer query.Close()
	if s.lastEntry == nil {
		query.Error(errors.NewNotFound("map is empty"))
	} else {
		query.Output(&indexedmapprotocolv1.LastEntryOutput{
			Entry: newStateMachineEntry(s.lastEntry.IndexedMapEntry),
		})
	}
}

func (s *indexedMapStateMachine) NextEntry(query statemachine.Query[*indexedmapprotocolv1.NextEntryInput, *indexedmapprotocolv1.NextEntryOutput]) {
	defer query.Close()
	var nextEntry *LinkedMapEntryValue
	if entry, ok := s.indexes[query.Input().Index]; ok {
		nextEntry = entry.Next
	} else {
		entry = s.firstEntry
		for entry != nil {
			if entry.Index > query.Input().Index {
				nextEntry = entry
				break
			}
			entry = entry.Next
		}
	}
	if nextEntry == nil {
		query.Error(errors.NewNotFound("map is empty"))
		return
	} else {
		query.Output(&indexedmapprotocolv1.NextEntryOutput{
			Entry: newStateMachineEntry(nextEntry.IndexedMapEntry),
		})
	}
}

func (s *indexedMapStateMachine) PrevEntry(query statemachine.Query[*indexedmapprotocolv1.PrevEntryInput, *indexedmapprotocolv1.PrevEntryOutput]) {
	defer query.Close()
	var prevEntry *LinkedMapEntryValue
	if entry, ok := s.indexes[query.Input().Index]; ok {
		prevEntry = entry.Prev
	} else {
		entry = s.lastEntry
		for entry != nil {
			if entry.Index < query.Input().Index {
				prevEntry = entry
				break
			}
			entry = entry.Prev
		}
	}
	if prevEntry == nil {
		query.Error(errors.NewNotFound("map is empty"))
		return
	} else {
		query.Output(&indexedmapprotocolv1.PrevEntryOutput{
			Entry: newStateMachineEntry(prevEntry.IndexedMapEntry),
		})
	}
}

func (s *indexedMapStateMachine) Entries(query statemachine.Query[*indexedmapprotocolv1.EntriesInput, *indexedmapprotocolv1.EntriesOutput]) {
	defer query.Close()
	entry := s.firstEntry
	for entry != nil {
		query.Output(&indexedmapprotocolv1.EntriesOutput{
			Entry: *newStateMachineEntry(entry.IndexedMapEntry),
		})
		entry = entry.Next
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

func (s *indexedMapStateMachine) notify(entry *LinkedMapEntryValue, event *indexedmapprotocolv1.Event) {
	for proposalID, listener := range s.streams {
		if listener.Key == "" || listener.Key == event.Key {
			proposal, ok := s.IndexedMapContext.Events().Get(proposalID)
			if ok {
				proposal.Output(&indexedmapprotocolv1.EventsOutput{
					Event: *event,
				})
			} else {
				delete(s.streams, proposalID)
			}
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&indexedmapprotocolv1.EntriesOutput{
			Entry: *newStateMachineEntry(entry.IndexedMapEntry),
		})
	}
}

func (s *indexedMapStateMachine) scheduleTTL(entry *LinkedMapEntryValue) {
	s.cancelTTL(entry.Key)
	if entry.Value.Expire != nil {
		s.timers[entry.Key] = s.Scheduler().Schedule(*entry.Value.Expire, func() {
			// Delete the entry from the key/index maps
			delete(s.keys, entry.Key)
			delete(s.indexes, entry.Index)

			// Update links for previous and next entries
			if entry.Prev != nil {
				entry.Prev.Next = entry.Next
			} else {
				s.firstEntry = entry.Next
			}
			if entry.Next != nil {
				entry.Next.Prev = entry.Prev
			} else {
				s.lastEntry = entry.Prev
			}

			// Notify watchers of the removal
			s.notify(entry, &indexedmapprotocolv1.Event{
				Key:   entry.Key,
				Index: entry.Index,
				Event: &indexedmapprotocolv1.Event_Removed_{
					Removed: &indexedmapprotocolv1.Event_Removed{
						Value: indexedmapprotocolv1.Value{
							Value:   entry.Value.Value,
							Version: entry.Value.Version,
						},
						Expired: true,
					},
				},
			})
		})
	}
}

func (s *indexedMapStateMachine) cancelTTL(key string) {
	ttlCancelFunc, ok := s.timers[key]
	if ok {
		ttlCancelFunc()
	}
}

func newStateMachineEntry(entry *indexedmapprotocolv1.IndexedMapEntry) *indexedmapprotocolv1.Entry {
	return &indexedmapprotocolv1.Entry{
		Key:   entry.Key,
		Index: entry.Index,
		Value: newStateMachineValue(entry.Value),
	}
}

func newStateMachineValue(value *indexedmapprotocolv1.IndexedMapValue) *indexedmapprotocolv1.Value {
	if value == nil {
		return nil
	}
	return &indexedmapprotocolv1.Value{
		Value:   value.Value,
		Version: value.Version,
	}
}
