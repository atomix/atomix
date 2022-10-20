// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var valueCodec = statemachine.NewCodec[*ValueInput, *ValueOutput](
	func(bytes []byte) (*ValueInput, error) {
		input := &ValueInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *ValueOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm ValueStateMachine) statemachine.Executor[*ValueInput, *ValueOutput] {
	executor := &ValueExecutor{
		ValueStateMachine: sm,
	}
	executor.init()
	return executor
}

type ValueExecutor struct {
	ValueStateMachine
	set    statemachine.Proposer[*ValueInput, *ValueOutput, *SetInput, *SetOutput]
	insert statemachine.Proposer[*ValueInput, *ValueOutput, *InsertInput, *InsertOutput]
	update statemachine.Proposer[*ValueInput, *ValueOutput, *UpdateInput, *UpdateOutput]
	delete statemachine.Proposer[*ValueInput, *ValueOutput, *DeleteInput, *DeleteOutput]
	events statemachine.Proposer[*ValueInput, *ValueOutput, *EventsInput, *EventsOutput]
	get    statemachine.Querier[*ValueInput, *ValueOutput, *GetInput, *GetOutput]
	watch  statemachine.Querier[*ValueInput, *ValueOutput, *WatchInput, *WatchOutput]
}

func (s *ValueExecutor) init() {
	s.set = statemachine.NewProposer[*ValueInput, *ValueOutput, *SetInput, *SetOutput]("Set").
		Decoder(func(input *ValueInput) (*SetInput, bool) {
			if put, ok := input.Input.(*ValueInput_Set); ok {
				return put.Set, true
			}
			return nil, false
		}).
		Encoder(func(output *SetOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Set{
					Set: output,
				},
			}
		}).
		Build(s.Set)
	s.insert = statemachine.NewProposer[*ValueInput, *ValueOutput, *InsertInput, *InsertOutput]("Insert").
		Decoder(func(input *ValueInput) (*InsertInput, bool) {
			if put, ok := input.Input.(*ValueInput_Insert); ok {
				return put.Insert, true
			}
			return nil, false
		}).
		Encoder(func(output *InsertOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Insert{
					Insert: output,
				},
			}
		}).
		Build(s.Insert)
	s.update = statemachine.NewProposer[*ValueInput, *ValueOutput, *UpdateInput, *UpdateOutput]("Update").
		Decoder(func(input *ValueInput) (*UpdateInput, bool) {
			if update, ok := input.Input.(*ValueInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *UpdateOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.delete = statemachine.NewProposer[*ValueInput, *ValueOutput, *DeleteInput, *DeleteOutput]("Delete").
		Decoder(func(input *ValueInput) (*DeleteInput, bool) {
			if remove, ok := input.Input.(*ValueInput_Delete); ok {
				return remove.Delete, true
			}
			return nil, false
		}).
		Encoder(func(output *DeleteOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Delete{
					Delete: output,
				},
			}
		}).
		Build(s.Delete)
	s.events = statemachine.NewProposer[*ValueInput, *ValueOutput, *EventsInput, *EventsOutput]("Events").
		Decoder(func(input *ValueInput) (*EventsInput, bool) {
			if events, ok := input.Input.(*ValueInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *EventsOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.get = statemachine.NewQuerier[*ValueInput, *ValueOutput, *GetInput, *GetOutput]("Get").
		Decoder(func(input *ValueInput) (*GetInput, bool) {
			if get, ok := input.Input.(*ValueInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.watch = statemachine.NewQuerier[*ValueInput, *ValueOutput, *WatchInput, *WatchOutput]("Watch").
		Decoder(func(input *ValueInput) (*WatchInput, bool) {
			if entries, ok := input.Input.(*ValueInput_Watch); ok {
				return entries.Watch, true
			}
			return nil, false
		}).
		Encoder(func(output *WatchOutput) *ValueOutput {
			return &ValueOutput{
				Output: &ValueOutput_Watch{
					Watch: output,
				},
			}
		}).
		Build(s.Watch)
}

func (s *ValueExecutor) Propose(proposal statemachine.Proposal[*ValueInput, *ValueOutput]) {
	switch proposal.Input().Input.(type) {
	case *ValueInput_Set:
		s.set(proposal)
	case *ValueInput_Insert:
		s.insert(proposal)
	case *ValueInput_Update:
		s.update(proposal)
	case *ValueInput_Delete:
		s.delete(proposal)
	case *ValueInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *ValueExecutor) Query(query statemachine.Query[*ValueInput, *ValueOutput]) {
	switch query.Input().Input.(type) {
	case *ValueInput_Get:
		s.get(query)
	case *ValueInput_Watch:
		s.watch(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
