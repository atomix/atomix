// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"sync"
)

const Service = "atomix.runtime.multimap.v1.MultiMap"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*MultiMapInput, *MultiMapOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*MultiMapInput, *MultiMapOutput](Service, multiMapCodec,
	func(context statemachine.PrimitiveContext[*MultiMapInput, *MultiMapOutput]) statemachine.Executor[*MultiMapInput, *MultiMapOutput] {
		return newExecutor(NewMultiMapStateMachine(context))
	})

type MultiMapContext interface {
	statemachine.PrimitiveContext[*MultiMapInput, *MultiMapOutput]
	Events() statemachine.Proposals[*EventsInput, *EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*MultiMapInput, *MultiMapOutput]) MultiMapContext {
	return &multiMapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*MultiMapInput, *MultiMapOutput, *EventsInput, *EventsOutput](context).
			Decoder(func(input *MultiMapInput) (*EventsInput, bool) {
				if events, ok := input.Input.(*MultiMapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *EventsOutput) *MultiMapOutput {
				return &MultiMapOutput{
					Output: &MultiMapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type multiMapContext struct {
	statemachine.PrimitiveContext[*MultiMapInput, *MultiMapOutput]
	events statemachine.Proposals[*EventsInput, *EventsOutput]
}

func (c *multiMapContext) Events() statemachine.Proposals[*EventsInput, *EventsOutput] {
	return c.events
}

type MultiMapStateMachine interface {
	statemachine.Context[*MultiMapInput, *MultiMapOutput]
	statemachine.Recoverable
	Put(statemachine.Proposal[*PutInput, *PutOutput])
	PutAll(statemachine.Proposal[*PutAllInput, *PutAllOutput])
	PutEntries(statemachine.Proposal[*PutEntriesInput, *PutEntriesOutput])
	Replace(statemachine.Proposal[*ReplaceInput, *ReplaceOutput])
	Remove(statemachine.Proposal[*RemoveInput, *RemoveOutput])
	RemoveAll(statemachine.Proposal[*RemoveAllInput, *RemoveAllOutput])
	RemoveEntries(statemachine.Proposal[*RemoveEntriesInput, *RemoveEntriesOutput])
	Clear(statemachine.Proposal[*ClearInput, *ClearOutput])
	Events(statemachine.Proposal[*EventsInput, *EventsOutput])
	Size(statemachine.Query[*SizeInput, *SizeOutput])
	Contains(statemachine.Query[*ContainsInput, *ContainsOutput])
	Get(statemachine.Query[*GetInput, *GetOutput])
	Entries(statemachine.Query[*EntriesInput, *EntriesOutput])
}

func NewMultiMapStateMachine(context statemachine.PrimitiveContext[*MultiMapInput, *MultiMapOutput]) MultiMapStateMachine {
	return &multiMapStateMachine{
		MultiMapContext: newContext(context),
		listeners:       make(map[statemachine.ProposalID]*MultiMapListener),
		entries:         make(map[string]map[string]bool),
		watchers:        make(map[statemachine.QueryID]statemachine.Query[*EntriesInput, *EntriesOutput]),
	}
}

type multiMapStateMachine struct {
	MultiMapContext
	listeners map[statemachine.ProposalID]*MultiMapListener
	entries   map[string]map[string]bool
	watchers  map[statemachine.QueryID]statemachine.Query[*EntriesInput, *EntriesOutput]
	mu        sync.RWMutex
}

func (s *multiMapStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	s.Log().Infow("Persisting MultiMap to snapshot")
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
	for key, values := range s.entries {
		if err := writer.WriteString(key); err != nil {
			return err
		}
		if err := writer.WriteVarInt(len(values)); err != nil {
			return err
		}
		for value := range values {
			if err := writer.WriteString(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *multiMapStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	s.Log().Infow("Recovering MultiMap from snapshot")
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		proposal, ok := s.MultiMapContext.Events().Get(statemachine.ProposalID(proposalID))
		if !ok {
			return errors.NewFault("cannot find proposal %d", proposalID)
		}
		listener := &MultiMapListener{}
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
		m, err := reader.ReadVarInt()
		if err != nil {
			return err
		}
		values := make(map[string]bool)
		for j := 0; j < m; j++ {
			value, err := reader.ReadString()
			if err != nil {
				return err
			}
			values[value] = true
		}
		s.entries[key] = values
	}
	return nil
}

func (s *multiMapStateMachine) Put(proposal statemachine.Proposal[*PutInput, *PutOutput]) {
	defer proposal.Close()

	values, ok := s.entries[proposal.Input().Key]
	if !ok {
		values = make(map[string]bool)
		s.entries[proposal.Input().Key] = values
	}

	if _, ok := values[proposal.Input().Value]; ok {
		proposal.Error(errors.NewAlreadyExists("entry already exists"))
		return
	}

	values[proposal.Input().Value] = true

	s.notify(&EventsOutput{
		Event: Event{
			Key: proposal.Input().Key,
			Event: &Event_Added_{
				Added: &Event_Added{
					Value: proposal.Input().Value,
				},
			},
		},
	})
	s.broadcast(proposal.Input().Key, values)

	proposal.Output(&PutOutput{})
}

func (s *multiMapStateMachine) PutAll(proposal statemachine.Proposal[*PutAllInput, *PutAllOutput]) {
	defer proposal.Close()

	values, ok := s.entries[proposal.Input().Key]
	if !ok {
		values = make(map[string]bool)
		s.entries[proposal.Input().Key] = values
	}

	updated := false
	for _, value := range proposal.Input().Values {
		if _, ok := values[value]; !ok {
			values[value] = true
			s.notify(&EventsOutput{
				Event: Event{
					Key: proposal.Input().Key,
					Event: &Event_Added_{
						Added: &Event_Added{
							Value: value,
						},
					},
				},
			})
			updated = true
		}
	}

	if updated {
		s.broadcast(proposal.Input().Key, values)
	}

	proposal.Output(&PutAllOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) PutEntries(proposal statemachine.Proposal[*PutEntriesInput, *PutEntriesOutput]) {
	defer proposal.Close()

	updated := false
	for _, entry := range proposal.Input().Entries {
		values, ok := s.entries[entry.Key]
		if !ok {
			values = make(map[string]bool)
			s.entries[entry.Key] = values
		}

		entryUpdated := false
		for _, value := range entry.Values {
			if _, ok := values[value]; !ok {
				values[value] = true
				s.notify(&EventsOutput{
					Event: Event{
						Key: entry.Key,
						Event: &Event_Added_{
							Added: &Event_Added{
								Value: value,
							},
						},
					},
				})
				entryUpdated = true
			}
		}
		if entryUpdated {
			s.broadcast(entry.Key, values)
			updated = true
		}
	}
	proposal.Output(&PutEntriesOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) Replace(proposal statemachine.Proposal[*ReplaceInput, *ReplaceOutput]) {
	defer proposal.Close()

	if len(proposal.Input().Values) == 0 {

	}

	oldValues, ok := s.entries[proposal.Input().Key]
	if !ok {
		oldValues = make(map[string]bool)
	}

	newValues := make(map[string]bool)
	s.entries[proposal.Input().Key] = newValues

	updated := false
	for _, value := range proposal.Input().Values {
		if _, ok := oldValues[value]; !ok {
			s.notify(&EventsOutput{
				Event: Event{
					Key: proposal.Input().Key,
					Event: &Event_Added_{
						Added: &Event_Added{
							Value: value,
						},
					},
				},
			})
			updated = true
		}
		newValues[value] = true
	}

	prevValues := make([]string, 0, len(oldValues))
	for value := range oldValues {
		if _, ok := newValues[value]; !ok {
			s.notify(&EventsOutput{
				Event: Event{
					Key: proposal.Input().Key,
					Event: &Event_Removed_{
						Removed: &Event_Removed{
							Value: value,
						},
					},
				},
			})
			updated = true
		}
		prevValues = append(prevValues, value)
	}

	if updated {
		s.broadcast(proposal.Input().Key, newValues)
	}

	proposal.Output(&ReplaceOutput{
		PrevValues: prevValues,
	})
}

func (s *multiMapStateMachine) Remove(proposal statemachine.Proposal[*RemoveInput, *RemoveOutput]) {
	defer proposal.Close()

	values, ok := s.entries[proposal.Input().Key]
	if !ok {
		proposal.Error(errors.NewNotFound("entry not found"))
		return
	}

	if _, ok := values[proposal.Input().Value]; !ok {
		proposal.Error(errors.NewNotFound("entry not found"))
		return
	}

	delete(values, proposal.Input().Value)

	if len(values) == 0 {
		delete(s.entries, proposal.Input().Key)
	}

	s.notify(&EventsOutput{
		Event: Event{
			Key: proposal.Input().Key,
			Event: &Event_Removed_{
				Removed: &Event_Removed{
					Value: proposal.Input().Value,
				},
			},
		},
	})
	s.broadcast(proposal.Input().Key, values)

	proposal.Output(&RemoveOutput{})
}

func (s *multiMapStateMachine) RemoveAll(proposal statemachine.Proposal[*RemoveAllInput, *RemoveAllOutput]) {
	defer proposal.Close()

	values, ok := s.entries[proposal.Input().Key]
	if !ok {
		proposal.Error(errors.NewNotFound("entry not found"))
		return
	}

	updated := false
	for _, value := range proposal.Input().Values {
		if _, ok := values[value]; ok {
			delete(values, value)
			s.notify(&EventsOutput{
				Event: Event{
					Key: proposal.Input().Key,
					Event: &Event_Removed_{
						Removed: &Event_Removed{
							Value: value,
						},
					},
				},
			})
			if len(values) == 0 {
				delete(s.entries, proposal.Input().Key)
			}
			s.broadcast(proposal.Input().Key, values)
			updated = true
		}
	}

	proposal.Output(&RemoveAllOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) RemoveEntries(proposal statemachine.Proposal[*RemoveEntriesInput, *RemoveEntriesOutput]) {
	defer proposal.Close()

	updated := false
	for _, entry := range proposal.Input().Entries {
		if values, ok := s.entries[entry.Key]; ok {
			entryUpdated := false
			for _, value := range entry.Values {
				if _, ok := values[value]; ok {
					delete(values, value)
					s.notify(&EventsOutput{
						Event: Event{
							Key: entry.Key,
							Event: &Event_Removed_{
								Removed: &Event_Removed{
									Value: value,
								},
							},
						},
					})
					entryUpdated = true
				}
			}
			if entryUpdated {
				if len(values) == 0 {
					delete(s.entries, entry.Key)
				}
				s.broadcast(entry.Key, values)
				updated = true
			}
		}
	}
	proposal.Output(&RemoveEntriesOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) Clear(proposal statemachine.Proposal[*ClearInput, *ClearOutput]) {
	defer proposal.Close()
	for key, values := range s.entries {
		for value := range values {
			s.notify(&EventsOutput{
				Event: Event{
					Key: key,
					Event: &Event_Removed_{
						Removed: &Event_Removed{
							Value: value,
						},
					},
				},
			})
		}
		delete(s.entries, key)
		s.broadcast(key, nil)
	}
	proposal.Output(&ClearOutput{})
}

func (s *multiMapStateMachine) Events(proposal statemachine.Proposal[*EventsInput, *EventsOutput]) {
	listener := &MultiMapListener{
		Key: proposal.Input().Key,
	}
	s.listeners[proposal.ID()] = listener
	proposal.Output(&EventsOutput{
		Event: Event{
			Key: proposal.Input().Key,
		},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *multiMapStateMachine) Size(query statemachine.Query[*SizeInput, *SizeOutput]) {
	defer query.Close()
	query.Output(&SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *multiMapStateMachine) Contains(query statemachine.Query[*ContainsInput, *ContainsOutput]) {
	defer query.Close()
	values, ok := s.entries[query.Input().Key]
	if !ok {
		query.Output(&ContainsOutput{
			Result: false,
		})
	} else if _, ok := values[query.Input().Value]; !ok {
		query.Output(&ContainsOutput{
			Result: false,
		})
	} else {
		query.Output(&ContainsOutput{
			Result: true,
		})
	}
}

func (s *multiMapStateMachine) Get(query statemachine.Query[*GetInput, *GetOutput]) {
	defer query.Close()
	values, ok := s.entries[query.Input().Key]
	if !ok {
		query.Output(&GetOutput{})
	} else {
		getValues := make([]string, 0, len(values))
		for value := range values {
			getValues = append(getValues, value)
		}
		query.Output(&GetOutput{
			Values: getValues,
		})
	}
}

func (s *multiMapStateMachine) Entries(query statemachine.Query[*EntriesInput, *EntriesOutput]) {
	for key, set := range s.entries {
		values := make([]string, 0, len(set))
		for value := range set {
			values = append(values, value)
		}
		query.Output(&EntriesOutput{
			Entry: Entry{
				Key:    key,
				Values: values,
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

func (s *multiMapStateMachine) notify(event *EventsOutput) {
	for proposalID, listener := range s.listeners {
		if listener.Key == "" || listener.Key == event.Event.Key {
			proposal, ok := s.MultiMapContext.Events().Get(proposalID)
			if ok {
				proposal.Output(event)
			} else {
				delete(s.listeners, proposalID)
			}
		}
	}
}

func (s *multiMapStateMachine) broadcast(key string, valuesSet map[string]bool) {
	values := make([]string, 0, len(valuesSet))
	for value := range valuesSet {
		values = append(values, value)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&EntriesOutput{
			Entry: Entry{
				Key:    key,
				Values: values,
			},
		})
	}
}
