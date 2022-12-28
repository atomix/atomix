// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/map/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var mapCodec = statemachine.NewCodec[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput](
	func(bytes []byte) (*mapprotocolv1.MapInput, error) {
		input := &mapprotocolv1.MapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *mapprotocolv1.MapOutput) ([]byte, error) {
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
	put    statemachine.Proposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.PutInput, *mapprotocolv1.PutOutput]
	insert statemachine.Proposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.InsertInput, *mapprotocolv1.InsertOutput]
	update statemachine.Proposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.UpdateInput, *mapprotocolv1.UpdateOutput]
	remove statemachine.Proposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.RemoveInput, *mapprotocolv1.RemoveOutput]
	clear  statemachine.Proposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.ClearInput, *mapprotocolv1.ClearOutput]
	events statemachine.Proposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput]
	size   statemachine.Querier[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.SizeInput, *mapprotocolv1.SizeOutput]
	get    statemachine.Querier[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.GetInput, *mapprotocolv1.GetOutput]
	list   statemachine.Querier[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput]
}

func (s *mapExecutor) init() {
	s.put = statemachine.NewProposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.PutInput, *mapprotocolv1.PutOutput]("Put").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.PutInput, bool) {
			if put, ok := input.Input.(*mapprotocolv1.MapInput_Put); ok {
				return put.Put, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.PutOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Put{
					Put: output,
				},
			}
		}).
		Build(s.sm.Put)
	s.insert = statemachine.NewProposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.InsertInput, *mapprotocolv1.InsertOutput]("Insert").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.InsertInput, bool) {
			if insert, ok := input.Input.(*mapprotocolv1.MapInput_Insert); ok {
				return insert.Insert, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.InsertOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Insert{
					Insert: output,
				},
			}
		}).
		Build(s.sm.Insert)
	s.update = statemachine.NewProposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.UpdateInput, *mapprotocolv1.UpdateOutput]("Update").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.UpdateInput, bool) {
			if update, ok := input.Input.(*mapprotocolv1.MapInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.UpdateOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.sm.Update)
	s.remove = statemachine.NewProposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.RemoveInput, *mapprotocolv1.RemoveOutput]("Remove").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.RemoveInput, bool) {
			if remove, ok := input.Input.(*mapprotocolv1.MapInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.RemoveOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.sm.Remove)
	s.clear = statemachine.NewProposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.ClearInput, *mapprotocolv1.ClearOutput]("Clear").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.ClearInput, bool) {
			if clear, ok := input.Input.(*mapprotocolv1.MapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.ClearOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.sm.Clear)
	s.events = statemachine.NewProposer[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.EventsInput, *mapprotocolv1.EventsOutput]("Events").
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
		Build(s.sm.Events)
	s.size = statemachine.NewQuerier[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.SizeInput, *mapprotocolv1.SizeOutput]("Size").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.SizeInput, bool) {
			if size, ok := input.Input.(*mapprotocolv1.MapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.SizeOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.sm.Size)
	s.get = statemachine.NewQuerier[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.GetInput, *mapprotocolv1.GetOutput]("Get").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.GetInput, bool) {
			if get, ok := input.Input.(*mapprotocolv1.MapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.GetOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.sm.Get)
	s.list = statemachine.NewQuerier[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput, *mapprotocolv1.EntriesInput, *mapprotocolv1.EntriesOutput]("Entries").
		Decoder(func(input *mapprotocolv1.MapInput) (*mapprotocolv1.EntriesInput, bool) {
			if entries, ok := input.Input.(*mapprotocolv1.MapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *mapprotocolv1.EntriesOutput) *mapprotocolv1.MapOutput {
			return &mapprotocolv1.MapOutput{
				Output: &mapprotocolv1.MapOutput_Entries{
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

func (s *mapExecutor) Propose(proposal statemachine.Proposal[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]) {
	switch proposal.Input().Input.(type) {
	case *mapprotocolv1.MapInput_Put:
		s.put(proposal)
	case *mapprotocolv1.MapInput_Insert:
		s.insert(proposal)
	case *mapprotocolv1.MapInput_Update:
		s.update(proposal)
	case *mapprotocolv1.MapInput_Remove:
		s.remove(proposal)
	case *mapprotocolv1.MapInput_Clear:
		s.clear(proposal)
	case *mapprotocolv1.MapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *mapExecutor) Query(query statemachine.Query[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]) {
	switch query.Input().Input.(type) {
	case *mapprotocolv1.MapInput_Size_:
		s.size(query)
	case *mapprotocolv1.MapInput_Get:
		s.get(query)
	case *mapprotocolv1.MapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
