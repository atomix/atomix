// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/set/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var setCodec = statemachine.NewCodec[*setprotocolv1.SetInput, *setprotocolv1.SetOutput](
	func(bytes []byte) (*setprotocolv1.SetInput, error) {
		input := &setprotocolv1.SetInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *setprotocolv1.SetOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm SetStateMachine) statemachine.Executor[*setprotocolv1.SetInput, *setprotocolv1.SetOutput] {
	executor := &SetExecutor{
		SetStateMachine: sm,
	}
	executor.init()
	return executor
}

type SetExecutor struct {
	SetStateMachine
	add      statemachine.Proposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.AddInput, *setprotocolv1.AddOutput]
	remove   statemachine.Proposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.RemoveInput, *setprotocolv1.RemoveOutput]
	clear    statemachine.Proposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.ClearInput, *setprotocolv1.ClearOutput]
	events   statemachine.Proposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.EventsInput, *setprotocolv1.EventsOutput]
	size     statemachine.Querier[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.SizeInput, *setprotocolv1.SizeOutput]
	contains statemachine.Querier[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.ContainsInput, *setprotocolv1.ContainsOutput]
	elements statemachine.Querier[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.ElementsInput, *setprotocolv1.ElementsOutput]
}

func (s *SetExecutor) init() {
	s.add = statemachine.NewProposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.AddInput, *setprotocolv1.AddOutput]("Add").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.AddInput, bool) {
			if put, ok := input.Input.(*setprotocolv1.SetInput_Add); ok {
				return put.Add, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.AddOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Add{
					Add: output,
				},
			}
		}).
		Build(s.Add)
	s.remove = statemachine.NewProposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.RemoveInput, *setprotocolv1.RemoveOutput]("Remove").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.RemoveInput, bool) {
			if remove, ok := input.Input.(*setprotocolv1.SetInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.RemoveOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.clear = statemachine.NewProposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.ClearInput, *setprotocolv1.ClearOutput]("Clear").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.ClearInput, bool) {
			if clear, ok := input.Input.(*setprotocolv1.SetInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.ClearOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.EventsInput, *setprotocolv1.EventsOutput]("Events").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.EventsInput, bool) {
			if events, ok := input.Input.(*setprotocolv1.SetInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.EventsOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.SizeInput, *setprotocolv1.SizeOutput]("Size").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.SizeInput, bool) {
			if size, ok := input.Input.(*setprotocolv1.SetInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.SizeOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.contains = statemachine.NewQuerier[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.ContainsInput, *setprotocolv1.ContainsOutput]("Contains").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.ContainsInput, bool) {
			if get, ok := input.Input.(*setprotocolv1.SetInput_Contains); ok {
				return get.Contains, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.ContainsOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Contains{
					Contains: output,
				},
			}
		}).
		Build(s.Contains)
	s.elements = statemachine.NewQuerier[*setprotocolv1.SetInput, *setprotocolv1.SetOutput, *setprotocolv1.ElementsInput, *setprotocolv1.ElementsOutput]("Elements").
		Decoder(func(input *setprotocolv1.SetInput) (*setprotocolv1.ElementsInput, bool) {
			if entries, ok := input.Input.(*setprotocolv1.SetInput_Elements); ok {
				return entries.Elements, true
			}
			return nil, false
		}).
		Encoder(func(output *setprotocolv1.ElementsOutput) *setprotocolv1.SetOutput {
			return &setprotocolv1.SetOutput{
				Output: &setprotocolv1.SetOutput_Elements{
					Elements: output,
				},
			}
		}).
		Build(s.Elements)
}

func (s *SetExecutor) Propose(proposal statemachine.Proposal[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]) {
	switch proposal.Input().Input.(type) {
	case *setprotocolv1.SetInput_Add:
		s.add(proposal)
	case *setprotocolv1.SetInput_Remove:
		s.remove(proposal)
	case *setprotocolv1.SetInput_Clear:
		s.clear(proposal)
	case *setprotocolv1.SetInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *SetExecutor) Query(query statemachine.Query[*setprotocolv1.SetInput, *setprotocolv1.SetOutput]) {
	switch query.Input().Input.(type) {
	case *setprotocolv1.SetInput_Size_:
		s.size(query)
	case *setprotocolv1.SetInput_Contains:
		s.contains(query)
	case *setprotocolv1.SetInput_Elements:
		s.elements(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
		query.Close()
	}
}
