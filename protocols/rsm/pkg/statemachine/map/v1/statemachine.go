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

const (
	version1 uint32 = 1
	version2 uint32 = 2
)

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
	Prepare(proposal statemachine.Proposal[*mapprotocolv1.PrepareInput, *mapprotocolv1.PrepareOutput])
	Commit(proposal statemachine.Proposal[*mapprotocolv1.CommitInput, *mapprotocolv1.CommitOutput])
	Abort(proposal statemachine.Proposal[*mapprotocolv1.AbortInput, *mapprotocolv1.AbortOutput])
	Apply(proposal statemachine.Proposal[*mapprotocolv1.ApplyInput, *mapprotocolv1.ApplyOutput])
	Events(proposal statemachine.Proposal[*mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput])
	Size(query statemachine.Query[*mapprotocolv1.SizeInput, *mapprotocolv1.SizeOutput])
	Get(query statemachine.Query[*mapprotocolv1.GetInput, *mapprotocolv1.GetOutput])
	Entries(query statemachine.Query[*mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput])
}

func NewMapStateMachine(context statemachine.PrimitiveContext[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]) MapStateMachine {
	return &mapStateMachine{
		MapContext:   newContext(context),
		listeners:    make(map[statemachine.ProposalID]*mapprotocolv1.MapListener),
		entries:      make(map[string]*mapprotocolv1.MapEntry),
		transactions: make(map[lockID][]mapprotocolv1.MapInput),
		locks:        make(map[string]lockID),
		sessions:     make(map[statemachine.SessionID]statemachine.CancelFunc),
		timers:       make(map[string]statemachine.CancelFunc),
	}
}

type lockID struct {
	sessionID   statemachine.SessionID
	sequenceNum protocol.SequenceNum
}

type mapStateMachine struct {
	MapContext
	listeners    map[statemachine.ProposalID]*mapprotocolv1.MapListener
	entries      map[string]*mapprotocolv1.MapEntry
	transactions map[lockID][]mapprotocolv1.MapInput
	locks        map[string]lockID
	sessions     map[statemachine.SessionID]statemachine.CancelFunc
	timers       map[string]statemachine.CancelFunc
	watchers     sync.Map
}

func (s *mapStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	s.Log().Infow("Persisting Map to snapshot")
	if err := writer.WriteVarUint32(version2); err != nil {
		return err
	}
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

	if err := writer.WriteVarInt(len(s.transactions)); err != nil {
		return err
	}
	for transactionID, inputs := range s.transactions {
		if err := writer.WriteVarUint64(uint64(transactionID.sessionID)); err != nil {
			return err
		}
		if err := writer.WriteVarUint64(uint64(transactionID.sequenceNum)); err != nil {
			return err
		}
		if err := writer.WriteVarInt(len(inputs)); err != nil {
			return err
		}
		for _, input := range inputs {
			if err := writer.WriteMessage(&input); err != nil {
				return err
			}
		}
	}

	if err := writer.WriteVarInt(len(s.locks)); err != nil {
		return err
	}
	for key, lock := range s.locks {
		if err := writer.WriteString(key); err != nil {
			return err
		}
		if err := writer.WriteVarUint64(uint64(lock.sessionID)); err != nil {
			return err
		}
		if err := writer.WriteVarUint64(uint64(lock.sequenceNum)); err != nil {
			return err
		}
	}
	return nil
}

func (s *mapStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	s.Log().Infow("Recovering Map from snapshot")
	version, err := reader.ReadVarUint32()
	if err != nil {
		return err
	}
	switch version {
	case version1, version2:
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
	}

	switch version {
	case version2:
		n, err := reader.ReadVarInt()
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			sessionID, err := reader.ReadVarUint64()
			if err != nil {
				return err
			}
			sequenceNum, err := reader.ReadVarUint64()
			if err != nil {
				return err
			}

			session, ok := s.Sessions().Get(statemachine.SessionID(sessionID))
			if !ok {
				return errors.NewInvalid("session %d not found", sessionID)
			}
			s.watchSession(session)

			m, err := reader.ReadVarInt()
			if err != nil {
				return err
			}
			inputs := make([]mapprotocolv1.MapInput, m)
			for j := 0; j < m; j++ {
				input := mapprotocolv1.MapInput{}
				if err := reader.ReadMessage(&input); err != nil {
					return err
				}
				inputs[j] = input
			}

			transactionID := lockID{
				sessionID:   statemachine.SessionID(sessionID),
				sequenceNum: protocol.SequenceNum(sequenceNum),
			}
			s.transactions[transactionID] = inputs
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
			sessionID, err := reader.ReadVarUint64()
			if err != nil {
				return err
			}
			sequenceNum, err := reader.ReadVarUint64()
			if err != nil {
				return err
			}
			session, ok := s.Sessions().Get(statemachine.SessionID(sessionID))
			if !ok {
				return errors.NewInvalid("session %d not found", sessionID)
			}
			s.watchSession(session)
			s.locks[key] = lockID{
				sessionID:   statemachine.SessionID(sessionID),
				sequenceNum: protocol.SequenceNum(sequenceNum),
			}
		}
	}
	return nil
}

func (s *mapStateMachine) watchSession(session statemachine.Session) {
	if _, ok := s.sessions[session.ID()]; !ok {
		s.sessions[session.ID()] = session.Watch(func(state statemachine.State) {
			if state == statemachine.Closed {
				for lockID, inputs := range s.transactions {
					if lockID.sessionID == session.ID() {
						for _, input := range inputs {
							switch i := input.Input.(type) {
							case *mapprotocolv1.MapInput_Put:
								s.applyPut(i.Put)
								delete(s.locks, i.Put.Key)
							case *mapprotocolv1.MapInput_Insert:
								s.applyInsert(i.Insert)
								delete(s.locks, i.Insert.Key)
							case *mapprotocolv1.MapInput_Update:
								s.applyUpdate(i.Update)
								delete(s.locks, i.Update.Key)
							case *mapprotocolv1.MapInput_Remove:
								s.applyRemove(i.Remove)
								delete(s.locks, i.Remove.Key)
							}
						}
					}
				}
				for key, lockID := range s.locks {
					if lockID.sessionID == session.ID() {
						delete(s.locks, key)
					}
				}
				delete(s.sessions, session.ID())
			}
		})
	}
}

func (s *mapStateMachine) Prepare(proposal statemachine.Proposal[*mapprotocolv1.PrepareInput, *mapprotocolv1.PrepareOutput]) {
	defer proposal.Close()

	keys := make(map[string]bool)
	for _, input := range proposal.Input().Inputs {
		switch i := input.Input.(type) {
		case *mapprotocolv1.MapInput_Put:
			if keys[i.Put.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Put.Key))
				return
			}
			if err := s.validatePut(i.Put); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Put.Key] = true
		case *mapprotocolv1.MapInput_Insert:
			if keys[i.Insert.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Insert.Key))
				return
			}
			if err := s.validateInsert(i.Insert); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Insert.Key] = true
		case *mapprotocolv1.MapInput_Update:
			if keys[i.Update.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Update.Key))
				return
			}
			if err := s.validateUpdate(i.Update); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Update.Key] = true
		case *mapprotocolv1.MapInput_Remove:
			if keys[i.Remove.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Remove.Key))
				return
			}
			if err := s.validateRemove(i.Remove); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Remove.Key] = true
		}
	}

	transactionID := lockID{
		sessionID:   proposal.Session().ID(),
		sequenceNum: proposal.Input().SequenceNum,
	}
	s.transactions[transactionID] = proposal.Input().Inputs
	for key := range keys {
		s.locks[key] = transactionID
	}
	s.watchSession(proposal.Session())
	proposal.Output(&mapprotocolv1.PrepareOutput{})
}

func (s *mapStateMachine) Commit(proposal statemachine.Proposal[*mapprotocolv1.CommitInput, *mapprotocolv1.CommitOutput]) {
	defer proposal.Close()

	transactionID := lockID{
		sessionID:   proposal.Session().ID(),
		sequenceNum: proposal.Input().SequenceNum,
	}
	inputs, ok := s.transactions[transactionID]
	if !ok {
		proposal.Error(errors.NewNotFound("transaction %d not found", proposal.Input().SequenceNum))
		return
	}

	outputs := make([]mapprotocolv1.MapOutput, 0, len(inputs))
	for _, input := range inputs {
		switch i := input.Input.(type) {
		case *mapprotocolv1.MapInput_Put:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Put{
					Put: s.applyPut(i.Put),
				},
			})
			delete(s.locks, i.Put.Key)
		case *mapprotocolv1.MapInput_Insert:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Insert{
					Insert: s.applyInsert(i.Insert),
				},
			})
			delete(s.locks, i.Insert.Key)
		case *mapprotocolv1.MapInput_Update:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Update{
					Update: s.applyUpdate(i.Update),
				},
			})
			delete(s.locks, i.Update.Key)
		case *mapprotocolv1.MapInput_Remove:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Remove{
					Remove: s.applyRemove(i.Remove),
				},
			})
			delete(s.locks, i.Remove.Key)
		}
	}
	delete(s.transactions, transactionID)

	proposal.Output(&mapprotocolv1.CommitOutput{
		Outputs: outputs,
	})
}

func (s *mapStateMachine) Abort(proposal statemachine.Proposal[*mapprotocolv1.AbortInput, *mapprotocolv1.AbortOutput]) {
	defer proposal.Close()

	transactionID := lockID{
		sessionID:   proposal.Session().ID(),
		sequenceNum: proposal.Input().SequenceNum,
	}
	if inputs, ok := s.transactions[transactionID]; ok {
		for _, input := range inputs {
			switch i := input.Input.(type) {
			case *mapprotocolv1.MapInput_Put:
				delete(s.locks, i.Put.Key)
			case *mapprotocolv1.MapInput_Insert:
				delete(s.locks, i.Insert.Key)
			case *mapprotocolv1.MapInput_Update:
				delete(s.locks, i.Update.Key)
			case *mapprotocolv1.MapInput_Remove:
				delete(s.locks, i.Remove.Key)
			}
		}
		delete(s.transactions, transactionID)
	}
	proposal.Output(&mapprotocolv1.AbortOutput{})
}

func (s *mapStateMachine) Apply(proposal statemachine.Proposal[*mapprotocolv1.ApplyInput, *mapprotocolv1.ApplyOutput]) {
	defer proposal.Close()

	keys := make(map[string]bool)
	for _, input := range proposal.Input().Inputs {
		switch i := input.Input.(type) {
		case *mapprotocolv1.MapInput_Put:
			if keys[i.Put.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Put.Key))
				return
			}
			if err := s.validatePut(i.Put); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Put.Key] = true
		case *mapprotocolv1.MapInput_Insert:
			if keys[i.Insert.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Insert.Key))
				return
			}
			if err := s.validateInsert(i.Insert); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Insert.Key] = true
		case *mapprotocolv1.MapInput_Update:
			if keys[i.Update.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Update.Key))
				return
			}
			if err := s.validateUpdate(i.Update); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Update.Key] = true
		case *mapprotocolv1.MapInput_Remove:
			if keys[i.Remove.Key] {
				proposal.Error(errors.NewConflict("multiple modifications of key %s in same transaction", i.Remove.Key))
				return
			}
			if err := s.validateRemove(i.Remove); err != nil {
				proposal.Error(err)
				return
			}
			keys[i.Remove.Key] = true
		}
	}

	outputs := make([]mapprotocolv1.MapOutput, 0, len(proposal.Input().Inputs))
	for _, input := range proposal.Input().Inputs {
		switch i := input.Input.(type) {
		case *mapprotocolv1.MapInput_Put:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Put{
					Put: s.applyPut(i.Put),
				},
			})
			delete(s.locks, i.Put.Key)
		case *mapprotocolv1.MapInput_Insert:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Insert{
					Insert: s.applyInsert(i.Insert),
				},
			})
			delete(s.locks, i.Insert.Key)
		case *mapprotocolv1.MapInput_Update:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Update{
					Update: s.applyUpdate(i.Update),
				},
			})
			delete(s.locks, i.Update.Key)
		case *mapprotocolv1.MapInput_Remove:
			outputs = append(outputs, mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Remove{
					Remove: s.applyRemove(i.Remove),
				},
			})
			delete(s.locks, i.Remove.Key)
		}
	}

	proposal.Output(&mapprotocolv1.ApplyOutput{
		Outputs: outputs,
	})
}

func (s *mapStateMachine) Put(proposal statemachine.Proposal[*mapprotocolv1.PutInput, *mapprotocolv1.PutOutput]) {
	defer proposal.Close()
	if err := s.validatePut(proposal.Input()); err != nil {
		proposal.Error(err)
	} else {
		proposal.Output(s.applyPut(proposal.Input()))
	}
}

func (s *mapStateMachine) validatePut(input *mapprotocolv1.PutInput) error {
	if _, ok := s.locks[input.Key]; ok {
		return errors.NewConflict("key %s is locked by another transaction", input.Key)
	}
	oldEntry := s.entries[input.Key]
	if input.PrevIndex > 0 && (oldEntry == nil || oldEntry.Value.Index != input.PrevIndex) {
		return errors.NewConflict("entry index %d does not match update index %d", oldEntry.Value.Index, input.PrevIndex)
	}
	return nil
}

func (s *mapStateMachine) applyPut(input *mapprotocolv1.PutInput) *mapprotocolv1.PutOutput {
	oldEntry := s.entries[input.Key]

	// If the value is equal to the current value, return a no-op.
	if oldEntry != nil && bytes.Equal(oldEntry.Value.Value, input.Value) {
		return &mapprotocolv1.PutOutput{
			Index: oldEntry.Value.Index,
		}
	}

	// Create a new entry and increment the revision number
	newEntry := &mapprotocolv1.MapEntry{
		Key: input.Key,
		Value: &mapprotocolv1.MapValue{
			Value: input.Value,
			Index: s.Index(),
		},
	}
	if input.TTL != nil {
		expire := s.Scheduler().Time().Add(*input.TTL)
		newEntry.Value.Expire = &expire
	}

	// Create a new entry value and set it in the map.
	s.entries[input.Key] = newEntry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(input.Key, newEntry)

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
		return &mapprotocolv1.PutOutput{
			Index: newEntry.Value.Index,
			PrevValue: &mapprotocolv1.IndexedValue{
				Value: oldEntry.Value.Value,
				Index: oldEntry.Value.Index,
			},
		}
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
		return &mapprotocolv1.PutOutput{
			Index: newEntry.Value.Index,
		}
	}
}

func (s *mapStateMachine) Insert(proposal statemachine.Proposal[*mapprotocolv1.InsertInput, *mapprotocolv1.InsertOutput]) {
	defer proposal.Close()
	if err := s.validateInsert(proposal.Input()); err != nil {
		proposal.Error(err)
	} else {
		proposal.Output(s.applyInsert(proposal.Input()))
	}
}

func (s *mapStateMachine) validateInsert(input *mapprotocolv1.InsertInput) error {
	if _, ok := s.locks[input.Key]; ok {
		return errors.NewConflict("key %s is locked by another transaction", input.Key)
	}
	if _, ok := s.entries[input.Key]; ok {
		return errors.NewAlreadyExists("key '%s' already exists", input.Key)
	}
	return nil
}

func (s *mapStateMachine) applyInsert(input *mapprotocolv1.InsertInput) *mapprotocolv1.InsertOutput {
	// Create a new entry and increment the revision number
	newEntry := &mapprotocolv1.MapEntry{
		Key: input.Key,
		Value: &mapprotocolv1.MapValue{
			Value: input.Value,
			Index: s.Index(),
		},
	}
	if input.TTL != nil {
		expire := s.Scheduler().Time().Add(*input.TTL)
		newEntry.Value.Expire = &expire
	}

	// Create a new entry value and set it in the map.
	s.entries[input.Key] = newEntry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(input.Key, newEntry)

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

	return &mapprotocolv1.InsertOutput{
		Index: newEntry.Value.Index,
	}
}

func (s *mapStateMachine) Update(proposal statemachine.Proposal[*mapprotocolv1.UpdateInput, *mapprotocolv1.UpdateOutput]) {
	defer proposal.Close()
	if err := s.validateUpdate(proposal.Input()); err != nil {
		proposal.Error(err)
	} else {
		proposal.Output(s.applyUpdate(proposal.Input()))
	}
}

func (s *mapStateMachine) validateUpdate(input *mapprotocolv1.UpdateInput) error {
	if _, ok := s.locks[input.Key]; ok {
		return errors.NewConflict("key %s is locked by another transaction", input.Key)
	}
	oldEntry, ok := s.entries[input.Key]
	if !ok {
		return errors.NewNotFound("key '%s' not found", input.Key)
	}
	if input.PrevIndex > 0 && oldEntry.Value.Index != input.PrevIndex {
		return errors.NewConflict("entry index %d does not match update index %d", oldEntry.Value.Index, input.PrevIndex)
	}
	return nil
}

func (s *mapStateMachine) applyUpdate(input *mapprotocolv1.UpdateInput) *mapprotocolv1.UpdateOutput {
	oldEntry := s.entries[input.Key]

	// Create a new entry and increment the revision number
	newEntry := &mapprotocolv1.MapEntry{
		Key: input.Key,
		Value: &mapprotocolv1.MapValue{
			Value: input.Value,
			Index: s.Index(),
		},
	}
	if input.TTL != nil {
		expire := s.Scheduler().Time().Add(*input.TTL)
		newEntry.Value.Expire = &expire
	}

	// Create a new entry value and set it in the map.
	s.entries[input.Key] = newEntry

	// Schedule the timeout for the value if necessary.
	s.scheduleTTL(input.Key, newEntry)

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

	return &mapprotocolv1.UpdateOutput{
		Index: newEntry.Value.Index,
		PrevValue: mapprotocolv1.IndexedValue{
			Value: oldEntry.Value.Value,
			Index: oldEntry.Value.Index,
		},
	}
}

func (s *mapStateMachine) Remove(proposal statemachine.Proposal[*mapprotocolv1.RemoveInput, *mapprotocolv1.RemoveOutput]) {
	defer proposal.Close()
	if err := s.validateRemove(proposal.Input()); err != nil {
		proposal.Error(err)
	} else {
		proposal.Output(s.applyRemove(proposal.Input()))
	}
}

func (s *mapStateMachine) validateRemove(input *mapprotocolv1.RemoveInput) error {
	if _, ok := s.locks[input.Key]; ok {
		return errors.NewConflict("key %s is locked by another transaction", input.Key)
	}
	entry, ok := s.entries[input.Key]
	if !ok {
		return errors.NewNotFound("key '%s' not found", input.Key)
	}
	if input.PrevIndex > 0 && entry.Value.Index != input.PrevIndex {
		return errors.NewConflict("entry index %d does not match remove index %d", entry.Value.Index, input.PrevIndex)
	}
	return nil
}

func (s *mapStateMachine) applyRemove(input *mapprotocolv1.RemoveInput) *mapprotocolv1.RemoveOutput {
	entry := s.entries[input.Key]
	delete(s.entries, input.Key)

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

	return &mapprotocolv1.RemoveOutput{
		Value: mapprotocolv1.IndexedValue{
			Value: entry.Value.Value,
			Index: entry.Value.Index,
		},
	}
}

func (s *mapStateMachine) Clear(proposal statemachine.Proposal[*mapprotocolv1.ClearInput, *mapprotocolv1.ClearOutput]) {
	defer proposal.Close()
	if len(s.locks) > 0 {
		proposal.Error(errors.NewConflict("%d keys are locked by other transaction(s)", len(s.locks)))
		return
	}
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
		s.watchers.Store(query.ID(), query)
		query.Watch(func(state statemachine.QueryState) {
			if state != statemachine.Running {
				s.watchers.Delete(query.ID())
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

	s.watchers.Range(func(key, value any) bool {
		watcher := value.(statemachine.Query[*mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput])
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
		return true
	})
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
