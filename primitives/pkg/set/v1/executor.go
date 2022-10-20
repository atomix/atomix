// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var setCodec = statemachine.NewCodec[*SetInput, *SetOutput](
	func(bytes []byte) (*SetInput, error) {
		input := &SetInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *SetOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm SetStateMachine) statemachine.Executor[*SetInput, *SetOutput] {
	executor := &SetExecutor{
		SetStateMachine: sm,
	}
	executor.init()
	return executor
}

type SetExecutor struct {
	SetStateMachine
	add      statemachine.Proposer[*SetInput, *SetOutput, *AddInput, *AddOutput]
	remove   statemachine.Proposer[*SetInput, *SetOutput, *RemoveInput, *RemoveOutput]
	clear    statemachine.Proposer[*SetInput, *SetOutput, *ClearInput, *ClearOutput]
	events   statemachine.Proposer[*SetInput, *SetOutput, *EventsInput, *EventsOutput]
	size     statemachine.Querier[*SetInput, *SetOutput, *SizeInput, *SizeOutput]
	contains statemachine.Querier[*SetInput, *SetOutput, *ContainsInput, *ContainsOutput]
	elements statemachine.Querier[*SetInput, *SetOutput, *ElementsInput, *ElementsOutput]
}

func (s *SetExecutor) init() {
	s.add = statemachine.NewProposer[*SetInput, *SetOutput, *AddInput, *AddOutput]("Add").
		Decoder(func(input *SetInput) (*AddInput, bool) {
			if put, ok := input.Input.(*SetInput_Add); ok {
				return put.Add, true
			}
			return nil, false
		}).
		Encoder(func(output *AddOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Add{
					Add: output,
				},
			}
		}).
		Build(s.Add)
	s.remove = statemachine.NewProposer[*SetInput, *SetOutput, *RemoveInput, *RemoveOutput]("Remove").
		Decoder(func(input *SetInput) (*RemoveInput, bool) {
			if remove, ok := input.Input.(*SetInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.clear = statemachine.NewProposer[*SetInput, *SetOutput, *ClearInput, *ClearOutput]("Clear").
		Decoder(func(input *SetInput) (*ClearInput, bool) {
			if clear, ok := input.Input.(*SetInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *ClearOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*SetInput, *SetOutput, *EventsInput, *EventsOutput]("Events").
		Decoder(func(input *SetInput) (*EventsInput, bool) {
			if events, ok := input.Input.(*SetInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *EventsOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*SetInput, *SetOutput, *SizeInput, *SizeOutput]("Size").
		Decoder(func(input *SetInput) (*SizeInput, bool) {
			if size, ok := input.Input.(*SetInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *SizeOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.contains = statemachine.NewQuerier[*SetInput, *SetOutput, *ContainsInput, *ContainsOutput]("Contains").
		Decoder(func(input *SetInput) (*ContainsInput, bool) {
			if get, ok := input.Input.(*SetInput_Contains); ok {
				return get.Contains, true
			}
			return nil, false
		}).
		Encoder(func(output *ContainsOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Contains{
					Contains: output,
				},
			}
		}).
		Build(s.Contains)
	s.elements = statemachine.NewQuerier[*SetInput, *SetOutput, *ElementsInput, *ElementsOutput]("Elements").
		Decoder(func(input *SetInput) (*ElementsInput, bool) {
			if entries, ok := input.Input.(*SetInput_Elements); ok {
				return entries.Elements, true
			}
			return nil, false
		}).
		Encoder(func(output *ElementsOutput) *SetOutput {
			return &SetOutput{
				Output: &SetOutput_Elements{
					Elements: output,
				},
			}
		}).
		Build(s.Elements)
}

func (s *SetExecutor) Propose(proposal statemachine.Proposal[*SetInput, *SetOutput]) {
	switch proposal.Input().Input.(type) {
	case *SetInput_Add:
		s.add(proposal)
	case *SetInput_Remove:
		s.remove(proposal)
	case *SetInput_Clear:
		s.clear(proposal)
	case *SetInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *SetExecutor) Query(query statemachine.Query[*SetInput, *SetOutput]) {
	switch query.Input().Input.(type) {
	case *SetInput_Size_:
		s.size(query)
	case *SetInput_Contains:
		s.contains(query)
	case *SetInput_Elements:
		s.elements(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
		query.Close()
	}
}
