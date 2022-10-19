// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var stateMachineCodec = statemachine.NewCodec[*CounterInput, *CounterOutput](
	func(bytes []byte) (*CounterInput, error) {
		input := &CounterInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *CounterOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func NewExecutor(stateMachine CounterStateMachine) statemachine.Executor[*CounterInput, *CounterOutput] {
	executor := &counterExecutor{
		CounterStateMachine: stateMachine,
	}
	executor.init()
	return executor
}

type counterExecutor struct {
	CounterStateMachine
	set       statemachine.Proposer[*CounterInput, *CounterOutput, *SetInput, *SetOutput]
	update    statemachine.Proposer[*CounterInput, *CounterOutput, *UpdateInput, *UpdateOutput]
	increment statemachine.Proposer[*CounterInput, *CounterOutput, *IncrementInput, *IncrementOutput]
	decrement statemachine.Proposer[*CounterInput, *CounterOutput, *DecrementInput, *DecrementOutput]
	get       statemachine.Querier[*CounterInput, *CounterOutput, *GetInput, *GetOutput]
}

func (s *counterExecutor) init() {
	s.set = statemachine.NewProposer[*CounterInput, *CounterOutput, *SetInput, *SetOutput](s).
		Name("Set").
		Decoder(func(input *CounterInput) (*SetInput, bool) {
			if set, ok := input.Input.(*CounterInput_Set); ok {
				return set.Set, true
			}
			return nil, false
		}).
		Encoder(func(output *SetOutput) *CounterOutput {
			return &CounterOutput{
				Output: &CounterOutput_Set{
					Set: output,
				},
			}
		}).
		Build(s.Set)
	s.update = statemachine.NewProposer[*CounterInput, *CounterOutput, *UpdateInput, *UpdateOutput](s).
		Name("Update").
		Decoder(func(input *CounterInput) (*UpdateInput, bool) {
			if set, ok := input.Input.(*CounterInput_Update); ok {
				return set.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *UpdateOutput) *CounterOutput {
			return &CounterOutput{
				Output: &CounterOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.increment = statemachine.NewProposer[*CounterInput, *CounterOutput, *IncrementInput, *IncrementOutput](s).
		Name("Increment").
		Decoder(func(input *CounterInput) (*IncrementInput, bool) {
			if set, ok := input.Input.(*CounterInput_Increment); ok {
				return set.Increment, true
			}
			return nil, false
		}).
		Encoder(func(output *IncrementOutput) *CounterOutput {
			return &CounterOutput{
				Output: &CounterOutput_Increment{
					Increment: output,
				},
			}
		}).
		Build(s.Increment)
	s.decrement = statemachine.NewProposer[*CounterInput, *CounterOutput, *DecrementInput, *DecrementOutput](s).
		Name("Decrement").
		Decoder(func(input *CounterInput) (*DecrementInput, bool) {
			if set, ok := input.Input.(*CounterInput_Decrement); ok {
				return set.Decrement, true
			}
			return nil, false
		}).
		Encoder(func(output *DecrementOutput) *CounterOutput {
			return &CounterOutput{
				Output: &CounterOutput_Decrement{
					Decrement: output,
				},
			}
		}).
		Build(s.Decrement)
	s.get = statemachine.NewQuerier[*CounterInput, *CounterOutput, *GetInput, *GetOutput](s).
		Name("Get").
		Decoder(func(input *CounterInput) (*GetInput, bool) {
			if set, ok := input.Input.(*CounterInput_Get); ok {
				return set.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *CounterOutput {
			return &CounterOutput{
				Output: &CounterOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
}

func (s *counterExecutor) Propose(proposal statemachine.ManagedProposal[*CounterInput, *CounterOutput]) {
	switch proposal.Input().Input.(type) {
	case *CounterInput_Set:
		s.set.Call(proposal)
	case *CounterInput_Update:
		s.update.Call(proposal)
	case *CounterInput_Increment:
		s.increment.Call(proposal)
	case *CounterInput_Decrement:
		s.decrement.Call(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
	}
}

func (s *counterExecutor) Query(query statemachine.ManagedQuery[*CounterInput, *CounterOutput]) {
	switch query.Input().Input.(type) {
	case *CounterInput_Get:
		s.get.Call(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
