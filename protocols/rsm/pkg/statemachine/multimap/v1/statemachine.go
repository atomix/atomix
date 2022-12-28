// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	multimapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/multimap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"sync"
)

const (
	Name       = "MultiMap"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]) statemachine.PrimitiveStateMachine[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput] {
			return newExecutor(NewMultiMapStateMachine(context))
		}, multiMapCodec)
}

type MultiMapContext interface {
	statemachine.PrimitiveContext[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]
	Events() statemachine.Proposals[*multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput]
}

func newContext(context statemachine.PrimitiveContext[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]) MultiMapContext {
	return &multiMapContext{
		PrimitiveContext: context,
		events: statemachine.NewProposals[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput](context).
			Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.EventsInput, bool) {
				if events, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Events); ok {
					return events.Events, true
				}
				return nil, false
			}).
			Encoder(func(output *multimapprotocolv1.EventsOutput) *multimapprotocolv1.MultiMapOutput {
				return &multimapprotocolv1.MultiMapOutput{
					Output: &multimapprotocolv1.MultiMapOutput_Events{
						Events: output,
					},
				}
			}).
			Build(),
	}
}

type multiMapContext struct {
	statemachine.PrimitiveContext[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]
	events statemachine.Proposals[*multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput]
}

func (c *multiMapContext) Events() statemachine.Proposals[*multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput] {
	return c.events
}

type MultiMapStateMachine interface {
	statemachine.Context[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]
	statemachine.Recoverable
	Put(statemachine.Proposal[*multimapprotocolv1.PutInput, *multimapprotocolv1.PutOutput])
	PutAll(statemachine.Proposal[*multimapprotocolv1.PutAllInput, *multimapprotocolv1.PutAllOutput])
	PutEntries(statemachine.Proposal[*multimapprotocolv1.PutEntriesInput, *multimapprotocolv1.PutEntriesOutput])
	Replace(statemachine.Proposal[*multimapprotocolv1.ReplaceInput, *multimapprotocolv1.ReplaceOutput])
	Remove(statemachine.Proposal[*multimapprotocolv1.RemoveInput, *multimapprotocolv1.RemoveOutput])
	RemoveAll(statemachine.Proposal[*multimapprotocolv1.RemoveAllInput, *multimapprotocolv1.RemoveAllOutput])
	RemoveEntries(statemachine.Proposal[*multimapprotocolv1.RemoveEntriesInput, *multimapprotocolv1.RemoveEntriesOutput])
	Clear(statemachine.Proposal[*multimapprotocolv1.ClearInput, *multimapprotocolv1.ClearOutput])
	Events(statemachine.Proposal[*multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput])
	Size(statemachine.Query[*multimapprotocolv1.SizeInput, *multimapprotocolv1.SizeOutput])
	Contains(statemachine.Query[*multimapprotocolv1.ContainsInput, *multimapprotocolv1.ContainsOutput])
	Get(statemachine.Query[*multimapprotocolv1.GetInput, *multimapprotocolv1.GetOutput])
	Entries(statemachine.Query[*multimapprotocolv1.EntriesInput, *multimapprotocolv1.EntriesOutput])
}

func NewMultiMapStateMachine(context statemachine.PrimitiveContext[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]) MultiMapStateMachine {
	return &multiMapStateMachine{
		MultiMapContext: newContext(context),
		listeners:       make(map[statemachine.ProposalID]*multimapprotocolv1.MultiMapListener),
		entries:         make(map[string]map[string]bool),
		watchers:        make(map[statemachine.QueryID]statemachine.Query[*multimapprotocolv1.EntriesInput, *multimapprotocolv1.EntriesOutput]),
	}
}

type multiMapStateMachine struct {
	MultiMapContext
	listeners map[statemachine.ProposalID]*multimapprotocolv1.MultiMapListener
	entries   map[string]map[string]bool
	watchers  map[statemachine.QueryID]statemachine.Query[*multimapprotocolv1.EntriesInput, *multimapprotocolv1.EntriesOutput]
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
		listener := &multimapprotocolv1.MultiMapListener{}
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

func (s *multiMapStateMachine) Put(proposal statemachine.Proposal[*multimapprotocolv1.PutInput, *multimapprotocolv1.PutOutput]) {
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

	s.notify(&multimapprotocolv1.EventsOutput{
		Event: multimapprotocolv1.Event{
			Key: proposal.Input().Key,
			Event: &multimapprotocolv1.Event_Added_{
				Added: &multimapprotocolv1.Event_Added{
					Value: proposal.Input().Value,
				},
			},
		},
	})
	s.broadcast(proposal.Input().Key, values)

	proposal.Output(&multimapprotocolv1.PutOutput{})
}

func (s *multiMapStateMachine) PutAll(proposal statemachine.Proposal[*multimapprotocolv1.PutAllInput, *multimapprotocolv1.PutAllOutput]) {
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
			s.notify(&multimapprotocolv1.EventsOutput{
				Event: multimapprotocolv1.Event{
					Key: proposal.Input().Key,
					Event: &multimapprotocolv1.Event_Added_{
						Added: &multimapprotocolv1.Event_Added{
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

	proposal.Output(&multimapprotocolv1.PutAllOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) PutEntries(proposal statemachine.Proposal[*multimapprotocolv1.PutEntriesInput, *multimapprotocolv1.PutEntriesOutput]) {
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
				s.notify(&multimapprotocolv1.EventsOutput{
					Event: multimapprotocolv1.Event{
						Key: entry.Key,
						Event: &multimapprotocolv1.Event_Added_{
							Added: &multimapprotocolv1.Event_Added{
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
	proposal.Output(&multimapprotocolv1.PutEntriesOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) Replace(proposal statemachine.Proposal[*multimapprotocolv1.ReplaceInput, *multimapprotocolv1.ReplaceOutput]) {
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
			s.notify(&multimapprotocolv1.EventsOutput{
				Event: multimapprotocolv1.Event{
					Key: proposal.Input().Key,
					Event: &multimapprotocolv1.Event_Added_{
						Added: &multimapprotocolv1.Event_Added{
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
			s.notify(&multimapprotocolv1.EventsOutput{
				Event: multimapprotocolv1.Event{
					Key: proposal.Input().Key,
					Event: &multimapprotocolv1.Event_Removed_{
						Removed: &multimapprotocolv1.Event_Removed{
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

	proposal.Output(&multimapprotocolv1.ReplaceOutput{
		PrevValues: prevValues,
	})
}

func (s *multiMapStateMachine) Remove(proposal statemachine.Proposal[*multimapprotocolv1.RemoveInput, *multimapprotocolv1.RemoveOutput]) {
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

	s.notify(&multimapprotocolv1.EventsOutput{
		Event: multimapprotocolv1.Event{
			Key: proposal.Input().Key,
			Event: &multimapprotocolv1.Event_Removed_{
				Removed: &multimapprotocolv1.Event_Removed{
					Value: proposal.Input().Value,
				},
			},
		},
	})
	s.broadcast(proposal.Input().Key, values)

	proposal.Output(&multimapprotocolv1.RemoveOutput{})
}

func (s *multiMapStateMachine) RemoveAll(proposal statemachine.Proposal[*multimapprotocolv1.RemoveAllInput, *multimapprotocolv1.RemoveAllOutput]) {
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
			s.notify(&multimapprotocolv1.EventsOutput{
				Event: multimapprotocolv1.Event{
					Key: proposal.Input().Key,
					Event: &multimapprotocolv1.Event_Removed_{
						Removed: &multimapprotocolv1.Event_Removed{
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

	proposal.Output(&multimapprotocolv1.RemoveAllOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) RemoveEntries(proposal statemachine.Proposal[*multimapprotocolv1.RemoveEntriesInput, *multimapprotocolv1.RemoveEntriesOutput]) {
	defer proposal.Close()

	updated := false
	for _, entry := range proposal.Input().Entries {
		if values, ok := s.entries[entry.Key]; ok {
			entryUpdated := false
			for _, value := range entry.Values {
				if _, ok := values[value]; ok {
					delete(values, value)
					s.notify(&multimapprotocolv1.EventsOutput{
						Event: multimapprotocolv1.Event{
							Key: entry.Key,
							Event: &multimapprotocolv1.Event_Removed_{
								Removed: &multimapprotocolv1.Event_Removed{
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
	proposal.Output(&multimapprotocolv1.RemoveEntriesOutput{
		Updated: updated,
	})
}

func (s *multiMapStateMachine) Clear(proposal statemachine.Proposal[*multimapprotocolv1.ClearInput, *multimapprotocolv1.ClearOutput]) {
	defer proposal.Close()
	for key, values := range s.entries {
		for value := range values {
			s.notify(&multimapprotocolv1.EventsOutput{
				Event: multimapprotocolv1.Event{
					Key: key,
					Event: &multimapprotocolv1.Event_Removed_{
						Removed: &multimapprotocolv1.Event_Removed{
							Value: value,
						},
					},
				},
			})
		}
		delete(s.entries, key)
		s.broadcast(key, nil)
	}
	proposal.Output(&multimapprotocolv1.ClearOutput{})
}

func (s *multiMapStateMachine) Events(proposal statemachine.Proposal[*multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput]) {
	listener := &multimapprotocolv1.MultiMapListener{
		Key: proposal.Input().Key,
	}
	s.listeners[proposal.ID()] = listener
	proposal.Output(&multimapprotocolv1.EventsOutput{
		Event: multimapprotocolv1.Event{
			Key: proposal.Input().Key,
		},
	})
	proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			delete(s.listeners, proposal.ID())
		}
	})
}

func (s *multiMapStateMachine) Size(query statemachine.Query[*multimapprotocolv1.SizeInput, *multimapprotocolv1.SizeOutput]) {
	defer query.Close()
	query.Output(&multimapprotocolv1.SizeOutput{
		Size_: uint32(len(s.entries)),
	})
}

func (s *multiMapStateMachine) Contains(query statemachine.Query[*multimapprotocolv1.ContainsInput, *multimapprotocolv1.ContainsOutput]) {
	defer query.Close()
	values, ok := s.entries[query.Input().Key]
	if !ok {
		query.Output(&multimapprotocolv1.ContainsOutput{
			Result: false,
		})
	} else if _, ok := values[query.Input().Value]; !ok {
		query.Output(&multimapprotocolv1.ContainsOutput{
			Result: false,
		})
	} else {
		query.Output(&multimapprotocolv1.ContainsOutput{
			Result: true,
		})
	}
}

func (s *multiMapStateMachine) Get(query statemachine.Query[*multimapprotocolv1.GetInput, *multimapprotocolv1.GetOutput]) {
	defer query.Close()
	values, ok := s.entries[query.Input().Key]
	if !ok {
		query.Output(&multimapprotocolv1.GetOutput{})
	} else {
		getValues := make([]string, 0, len(values))
		for value := range values {
			getValues = append(getValues, value)
		}
		query.Output(&multimapprotocolv1.GetOutput{
			Values: getValues,
		})
	}
}

func (s *multiMapStateMachine) Entries(query statemachine.Query[*multimapprotocolv1.EntriesInput, *multimapprotocolv1.EntriesOutput]) {
	for key, set := range s.entries {
		values := make([]string, 0, len(set))
		for value := range set {
			values = append(values, value)
		}
		query.Output(&multimapprotocolv1.EntriesOutput{
			Entry: multimapprotocolv1.Entry{
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

func (s *multiMapStateMachine) notify(event *multimapprotocolv1.EventsOutput) {
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
		watcher.Output(&multimapprotocolv1.EntriesOutput{
			Entry: multimapprotocolv1.Entry{
				Key:    key,
				Values: values,
			},
		})
	}
}
