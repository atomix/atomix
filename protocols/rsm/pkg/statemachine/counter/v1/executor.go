// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/counter/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var stateMachineCodec = statemachine.NewCodec[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput](
	func(bytes []byte) (*counterprotocolv1.CounterInput, error) {
		input := &counterprotocolv1.CounterInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *counterprotocolv1.CounterOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(stateMachine CounterStateMachine) statemachine.Executor[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput] {
	executor := &counterExecutor{
		CounterStateMachine: stateMachine,
	}
	executor.init()
	return executor
}

type counterExecutor struct {
	CounterStateMachine
	set       statemachine.Proposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.SetInput, *counterprotocolv1.SetOutput]
	update    statemachine.Proposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.UpdateInput, *counterprotocolv1.UpdateOutput]
	increment statemachine.Proposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.IncrementInput, *counterprotocolv1.IncrementOutput]
	decrement statemachine.Proposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.DecrementInput, *counterprotocolv1.DecrementOutput]
	get       statemachine.Querier[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.GetInput, *counterprotocolv1.GetOutput]
}

func (s *counterExecutor) init() {
	s.set = statemachine.NewProposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.SetInput, *counterprotocolv1.SetOutput]("Set").
		Decoder(func(input *counterprotocolv1.CounterInput) (*counterprotocolv1.SetInput, bool) {
			if set, ok := input.Input.(*counterprotocolv1.CounterInput_Set); ok {
				return set.Set, true
			}
			return nil, false
		}).
		Encoder(func(output *counterprotocolv1.SetOutput) *counterprotocolv1.CounterOutput {
			return &counterprotocolv1.CounterOutput{
				Output: &counterprotocolv1.CounterOutput_Set{
					Set: output,
				},
			}
		}).
		Build(s.Set)
	s.update = statemachine.NewProposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.UpdateInput, *counterprotocolv1.UpdateOutput]("Update").
		Decoder(func(input *counterprotocolv1.CounterInput) (*counterprotocolv1.UpdateInput, bool) {
			if set, ok := input.Input.(*counterprotocolv1.CounterInput_Update); ok {
				return set.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *counterprotocolv1.UpdateOutput) *counterprotocolv1.CounterOutput {
			return &counterprotocolv1.CounterOutput{
				Output: &counterprotocolv1.CounterOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.increment = statemachine.NewProposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.IncrementInput, *counterprotocolv1.IncrementOutput]("Increment").
		Decoder(func(input *counterprotocolv1.CounterInput) (*counterprotocolv1.IncrementInput, bool) {
			if set, ok := input.Input.(*counterprotocolv1.CounterInput_Increment); ok {
				return set.Increment, true
			}
			return nil, false
		}).
		Encoder(func(output *counterprotocolv1.IncrementOutput) *counterprotocolv1.CounterOutput {
			return &counterprotocolv1.CounterOutput{
				Output: &counterprotocolv1.CounterOutput_Increment{
					Increment: output,
				},
			}
		}).
		Build(s.Increment)
	s.decrement = statemachine.NewProposer[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.DecrementInput, *counterprotocolv1.DecrementOutput]("Decrement").
		Decoder(func(input *counterprotocolv1.CounterInput) (*counterprotocolv1.DecrementInput, bool) {
			if set, ok := input.Input.(*counterprotocolv1.CounterInput_Decrement); ok {
				return set.Decrement, true
			}
			return nil, false
		}).
		Encoder(func(output *counterprotocolv1.DecrementOutput) *counterprotocolv1.CounterOutput {
			return &counterprotocolv1.CounterOutput{
				Output: &counterprotocolv1.CounterOutput_Decrement{
					Decrement: output,
				},
			}
		}).
		Build(s.Decrement)
	s.get = statemachine.NewQuerier[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput, *counterprotocolv1.GetInput, *counterprotocolv1.GetOutput]("Get").
		Decoder(func(input *counterprotocolv1.CounterInput) (*counterprotocolv1.GetInput, bool) {
			if set, ok := input.Input.(*counterprotocolv1.CounterInput_Get); ok {
				return set.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *counterprotocolv1.GetOutput) *counterprotocolv1.CounterOutput {
			return &counterprotocolv1.CounterOutput{
				Output: &counterprotocolv1.CounterOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
}

func (s *counterExecutor) Propose(proposal statemachine.Proposal[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]) {
	switch proposal.Input().Input.(type) {
	case *counterprotocolv1.CounterInput_Set:
		s.set(proposal)
	case *counterprotocolv1.CounterInput_Update:
		s.update(proposal)
	case *counterprotocolv1.CounterInput_Increment:
		s.increment(proposal)
	case *counterprotocolv1.CounterInput_Decrement:
		s.decrement(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
	}
}

func (s *counterExecutor) Query(query statemachine.Query[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]) {
	switch query.Input().Input.(type) {
	case *counterprotocolv1.CounterInput_Get:
		s.get(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
