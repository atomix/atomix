// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var stateMachineCodec = statemachine.NewCodec[*MapInput, *MapOutput](
	func(bytes []byte) (*MapInput, error) {
		input := &MapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *MapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm MapStateMachine) *mapExecutor {
	executor := &mapExecutor{
		sm: sm,
	}
	executor.init()
	return executor
}

type mapExecutor struct {
	sm     MapStateMachine
	put    statemachine.Proposer[*MapInput, *MapOutput, *PutInput, *PutOutput]
	insert statemachine.Proposer[*MapInput, *MapOutput, *InsertInput, *InsertOutput]
	update statemachine.Proposer[*MapInput, *MapOutput, *UpdateInput, *UpdateOutput]
	remove statemachine.Proposer[*MapInput, *MapOutput, *RemoveInput, *RemoveOutput]
	clear  statemachine.Proposer[*MapInput, *MapOutput, *ClearInput, *ClearOutput]
	events statemachine.Proposer[*MapInput, *MapOutput, *EventsInput, *EventsOutput]
	size   statemachine.Querier[*MapInput, *MapOutput, *SizeInput, *SizeOutput]
	get    statemachine.Querier[*MapInput, *MapOutput, *GetInput, *GetOutput]
	list   statemachine.Querier[*MapInput, *MapOutput, *EntriesInput, *EntriesOutput]
}

func (s *mapExecutor) init() {
	s.put = statemachine.NewProposer[*MapInput, *MapOutput, *PutInput, *PutOutput]("Put").
		Decoder(func(input *MapInput) (*PutInput, bool) {
			if put, ok := input.Input.(*MapInput_Put); ok {
				return put.Put, true
			}
			return nil, false
		}).
		Encoder(func(output *PutOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Put{
					Put: output,
				},
			}
		}).
		Build(s.sm.Put)
	s.insert = statemachine.NewProposer[*MapInput, *MapOutput, *InsertInput, *InsertOutput]("Insert").
		Decoder(func(input *MapInput) (*InsertInput, bool) {
			if insert, ok := input.Input.(*MapInput_Insert); ok {
				return insert.Insert, true
			}
			return nil, false
		}).
		Encoder(func(output *InsertOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Insert{
					Insert: output,
				},
			}
		}).
		Build(s.sm.Insert)
	s.update = statemachine.NewProposer[*MapInput, *MapOutput, *UpdateInput, *UpdateOutput]("Update").
		Decoder(func(input *MapInput) (*UpdateInput, bool) {
			if update, ok := input.Input.(*MapInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *UpdateOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.sm.Update)
	s.remove = statemachine.NewProposer[*MapInput, *MapOutput, *RemoveInput, *RemoveOutput]("Remove").
		Decoder(func(input *MapInput) (*RemoveInput, bool) {
			if remove, ok := input.Input.(*MapInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.sm.Remove)
	s.clear = statemachine.NewProposer[*MapInput, *MapOutput, *ClearInput, *ClearOutput]("Clear").
		Decoder(func(input *MapInput) (*ClearInput, bool) {
			if clear, ok := input.Input.(*MapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *ClearOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.sm.Clear)
	s.events = statemachine.NewProposer[*MapInput, *MapOutput, *EventsInput, *EventsOutput]("Events").
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
		Build(s.sm.Events)
	s.size = statemachine.NewQuerier[*MapInput, *MapOutput, *SizeInput, *SizeOutput]("Size").
		Decoder(func(input *MapInput) (*SizeInput, bool) {
			if size, ok := input.Input.(*MapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *SizeOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.sm.Size)
	s.get = statemachine.NewQuerier[*MapInput, *MapOutput, *GetInput, *GetOutput]("Get").
		Decoder(func(input *MapInput) (*GetInput, bool) {
			if get, ok := input.Input.(*MapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.sm.Get)
	s.list = statemachine.NewQuerier[*MapInput, *MapOutput, *EntriesInput, *EntriesOutput]("Entries").
		Decoder(func(input *MapInput) (*EntriesInput, bool) {
			if entries, ok := input.Input.(*MapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *EntriesOutput) *MapOutput {
			return &MapOutput{
				Output: &MapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.sm.Entries)
}

func (s *mapExecutor) Snapshot(writer *statemachine.SnapshotWriter) error {
	return s.sm.Snapshot(writer)
}

func (s *mapExecutor) Recover(reader *statemachine.SnapshotReader) error {
	return s.sm.Recover(reader)
}

func (s *mapExecutor) Propose(proposal statemachine.Proposal[*MapInput, *MapOutput]) {
	switch proposal.Input().Input.(type) {
	case *MapInput_Put:
		s.put(proposal)
	case *MapInput_Insert:
		s.insert(proposal)
	case *MapInput_Update:
		s.update(proposal)
	case *MapInput_Remove:
		s.remove(proposal)
	case *MapInput_Clear:
		s.clear(proposal)
	case *MapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *mapExecutor) Query(query statemachine.Query[*MapInput, *MapOutput]) {
	switch query.Input().Input.(type) {
	case *MapInput_Size_:
		s.size(query)
	case *MapInput_Get:
		s.get(query)
	case *MapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
