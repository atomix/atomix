// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/errors"
	countermapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/countermap/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/gogo/protobuf/proto"
)

var counterMapCodec = statemachine.NewCodec[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput](
	func(bytes []byte) (*countermapprotocolv1.CounterMapInput, error) {
		input := &countermapprotocolv1.CounterMapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *countermapprotocolv1.CounterMapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm CounterMapStateMachine) statemachine.PrimitiveStateMachine[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput] {
	executor := &CounterMapExecutor{
		CounterMapStateMachine: sm,
	}
	executor.init()
	return executor
}

type CounterMapExecutor struct {
	CounterMapStateMachine
	set       statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.SetInput, *countermapprotocolv1.SetOutput]
	insert    statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.InsertInput, *countermapprotocolv1.InsertOutput]
	update    statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.UpdateInput, *countermapprotocolv1.UpdateOutput]
	remove    statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.RemoveInput, *countermapprotocolv1.RemoveOutput]
	increment statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.IncrementInput, *countermapprotocolv1.IncrementOutput]
	decrement statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.DecrementInput, *countermapprotocolv1.DecrementOutput]
	clear     statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.ClearInput, *countermapprotocolv1.ClearOutput]
	events    statemachine.Proposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput]
	size      statemachine.Querier[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.SizeInput, *countermapprotocolv1.SizeOutput]
	get       statemachine.Querier[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.GetInput, *countermapprotocolv1.GetOutput]
	list      statemachine.Querier[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.EntriesInput, *countermapprotocolv1.EntriesOutput]
}

func (s *CounterMapExecutor) init() {
	s.set = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.SetInput, *countermapprotocolv1.SetOutput]("Set").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.SetInput, bool) {
			if put, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Set); ok {
				return put.Set, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.SetOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Set{
					Set: output,
				},
			}
		}).
		Build(s.Set)
	s.insert = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.InsertInput, *countermapprotocolv1.InsertOutput]("Insert").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.InsertInput, bool) {
			if insert, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Insert); ok {
				return insert.Insert, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.InsertOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Insert{
					Insert: output,
				},
			}
		}).
		Build(s.Insert)
	s.update = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.UpdateInput, *countermapprotocolv1.UpdateOutput]("Update").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.UpdateInput, bool) {
			if update, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.UpdateOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.remove = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.RemoveInput, *countermapprotocolv1.RemoveOutput]("Remove").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.RemoveInput, bool) {
			if remove, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.RemoveOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.increment = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.IncrementInput, *countermapprotocolv1.IncrementOutput]("Increment").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.IncrementInput, bool) {
			if remove, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Increment); ok {
				return remove.Increment, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.IncrementOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Increment{
					Increment: output,
				},
			}
		}).
		Build(s.Increment)
	s.decrement = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.DecrementInput, *countermapprotocolv1.DecrementOutput]("Decrement").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.DecrementInput, bool) {
			if remove, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Decrement); ok {
				return remove.Decrement, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.DecrementOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Decrement{
					Decrement: output,
				},
			}
		}).
		Build(s.Decrement)
	s.clear = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.ClearInput, *countermapprotocolv1.ClearOutput]("Clear").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.ClearInput, bool) {
			if clear, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.ClearOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.EventsInput, *countermapprotocolv1.EventsOutput]("Events").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.EventsInput, bool) {
			if events, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.EventsOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.SizeInput, *countermapprotocolv1.SizeOutput]("Size").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.SizeInput, bool) {
			if size, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.SizeOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.get = statemachine.NewQuerier[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.GetInput, *countermapprotocolv1.GetOutput]("Get").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.GetInput, bool) {
			if get, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.GetOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.list = statemachine.NewQuerier[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput, *countermapprotocolv1.EntriesInput, *countermapprotocolv1.EntriesOutput]("Entries").
		Decoder(func(input *countermapprotocolv1.CounterMapInput) (*countermapprotocolv1.EntriesInput, bool) {
			if entries, ok := input.Input.(*countermapprotocolv1.CounterMapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *countermapprotocolv1.EntriesOutput) *countermapprotocolv1.CounterMapOutput {
			return &countermapprotocolv1.CounterMapOutput{
				Output: &countermapprotocolv1.CounterMapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.Entries)
}

func (s *CounterMapExecutor) Propose(proposal statemachine.Proposal[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]) {
	switch proposal.Input().Input.(type) {
	case *countermapprotocolv1.CounterMapInput_Set:
		s.set(proposal)
	case *countermapprotocolv1.CounterMapInput_Insert:
		s.insert(proposal)
	case *countermapprotocolv1.CounterMapInput_Update:
		s.update(proposal)
	case *countermapprotocolv1.CounterMapInput_Increment:
		s.increment(proposal)
	case *countermapprotocolv1.CounterMapInput_Decrement:
		s.decrement(proposal)
	case *countermapprotocolv1.CounterMapInput_Remove:
		s.remove(proposal)
	case *countermapprotocolv1.CounterMapInput_Clear:
		s.clear(proposal)
	case *countermapprotocolv1.CounterMapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *CounterMapExecutor) Query(query statemachine.Query[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]) {
	switch query.Input().Input.(type) {
	case *countermapprotocolv1.CounterMapInput_Size_:
		s.size(query)
	case *countermapprotocolv1.CounterMapInput_Get:
		s.get(query)
	case *countermapprotocolv1.CounterMapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
