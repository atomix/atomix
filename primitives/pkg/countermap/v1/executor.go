// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var counterMapCodec = statemachine.NewCodec[*CounterMapInput, *CounterMapOutput](
	func(bytes []byte) (*CounterMapInput, error) {
		input := &CounterMapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *CounterMapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm CounterMapStateMachine) statemachine.Executor[*CounterMapInput, *CounterMapOutput] {
	executor := &CounterMapExecutor{
		CounterMapStateMachine: sm,
	}
	executor.init()
	return executor
}

type CounterMapExecutor struct {
	CounterMapStateMachine
	set       statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *SetInput, *SetOutput]
	insert    statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *InsertInput, *InsertOutput]
	update    statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *UpdateInput, *UpdateOutput]
	remove    statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *RemoveInput, *RemoveOutput]
	increment statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *IncrementInput, *IncrementOutput]
	decrement statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *DecrementInput, *DecrementOutput]
	clear     statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *ClearInput, *ClearOutput]
	events    statemachine.Proposer[*CounterMapInput, *CounterMapOutput, *EventsInput, *EventsOutput]
	size      statemachine.Querier[*CounterMapInput, *CounterMapOutput, *SizeInput, *SizeOutput]
	get       statemachine.Querier[*CounterMapInput, *CounterMapOutput, *GetInput, *GetOutput]
	list      statemachine.Querier[*CounterMapInput, *CounterMapOutput, *EntriesInput, *EntriesOutput]
}

func (s *CounterMapExecutor) init() {
	s.set = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *SetInput, *SetOutput]("Set").
		Decoder(func(input *CounterMapInput) (*SetInput, bool) {
			if put, ok := input.Input.(*CounterMapInput_Set); ok {
				return put.Set, true
			}
			return nil, false
		}).
		Encoder(func(output *SetOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Set{
					Set: output,
				},
			}
		}).
		Build(s.Set)
	s.insert = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *InsertInput, *InsertOutput]("Insert").
		Decoder(func(input *CounterMapInput) (*InsertInput, bool) {
			if insert, ok := input.Input.(*CounterMapInput_Insert); ok {
				return insert.Insert, true
			}
			return nil, false
		}).
		Encoder(func(output *InsertOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Insert{
					Insert: output,
				},
			}
		}).
		Build(s.Insert)
	s.update = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *UpdateInput, *UpdateOutput]("Update").
		Decoder(func(input *CounterMapInput) (*UpdateInput, bool) {
			if update, ok := input.Input.(*CounterMapInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *UpdateOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.remove = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *RemoveInput, *RemoveOutput]("Remove").
		Decoder(func(input *CounterMapInput) (*RemoveInput, bool) {
			if remove, ok := input.Input.(*CounterMapInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.increment = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *IncrementInput, *IncrementOutput]("Increment").
		Decoder(func(input *CounterMapInput) (*IncrementInput, bool) {
			if remove, ok := input.Input.(*CounterMapInput_Increment); ok {
				return remove.Increment, true
			}
			return nil, false
		}).
		Encoder(func(output *IncrementOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Increment{
					Increment: output,
				},
			}
		}).
		Build(s.Increment)
	s.decrement = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *DecrementInput, *DecrementOutput]("Decrement").
		Decoder(func(input *CounterMapInput) (*DecrementInput, bool) {
			if remove, ok := input.Input.(*CounterMapInput_Decrement); ok {
				return remove.Decrement, true
			}
			return nil, false
		}).
		Encoder(func(output *DecrementOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Decrement{
					Decrement: output,
				},
			}
		}).
		Build(s.Decrement)
	s.clear = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *ClearInput, *ClearOutput]("Clear").
		Decoder(func(input *CounterMapInput) (*ClearInput, bool) {
			if clear, ok := input.Input.(*CounterMapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *ClearOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*CounterMapInput, *CounterMapOutput, *EventsInput, *EventsOutput]("Events").
		Decoder(func(input *CounterMapInput) (*EventsInput, bool) {
			if events, ok := input.Input.(*CounterMapInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *EventsOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*CounterMapInput, *CounterMapOutput, *SizeInput, *SizeOutput]("Size").
		Decoder(func(input *CounterMapInput) (*SizeInput, bool) {
			if size, ok := input.Input.(*CounterMapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *SizeOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.get = statemachine.NewQuerier[*CounterMapInput, *CounterMapOutput, *GetInput, *GetOutput]("Get").
		Decoder(func(input *CounterMapInput) (*GetInput, bool) {
			if get, ok := input.Input.(*CounterMapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.list = statemachine.NewQuerier[*CounterMapInput, *CounterMapOutput, *EntriesInput, *EntriesOutput]("Entries").
		Decoder(func(input *CounterMapInput) (*EntriesInput, bool) {
			if entries, ok := input.Input.(*CounterMapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *EntriesOutput) *CounterMapOutput {
			return &CounterMapOutput{
				Output: &CounterMapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.Entries)
}

func (s *CounterMapExecutor) Propose(proposal statemachine.Proposal[*CounterMapInput, *CounterMapOutput]) {
	switch proposal.Input().Input.(type) {
	case *CounterMapInput_Set:
		s.set(proposal)
	case *CounterMapInput_Insert:
		s.insert(proposal)
	case *CounterMapInput_Update:
		s.update(proposal)
	case *CounterMapInput_Increment:
		s.increment(proposal)
	case *CounterMapInput_Decrement:
		s.decrement(proposal)
	case *CounterMapInput_Remove:
		s.remove(proposal)
	case *CounterMapInput_Clear:
		s.clear(proposal)
	case *CounterMapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *CounterMapExecutor) Query(query statemachine.Query[*CounterMapInput, *CounterMapOutput]) {
	switch query.Input().Input.(type) {
	case *CounterMapInput_Size_:
		s.size(query)
	case *CounterMapInput_Get:
		s.get(query)
	case *CounterMapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
