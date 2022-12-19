// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valueprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/value/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var valueCodec = statemachine.NewCodec[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput](
	func(bytes []byte) (*valueprotocolv1.ValueInput, error) {
		input := &valueprotocolv1.ValueInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *valueprotocolv1.ValueOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm ValueStateMachine) statemachine.PrimitiveStateMachine[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput] {
	executor := &ValueExecutor{
		ValueStateMachine: sm,
	}
	executor.init()
	return executor
}

type ValueExecutor struct {
	ValueStateMachine
	set    statemachine.Proposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.SetInput, *valueprotocolv1.SetOutput]
	insert statemachine.Proposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.InsertInput, *valueprotocolv1.InsertOutput]
	update statemachine.Proposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.UpdateInput, *valueprotocolv1.UpdateOutput]
	delete statemachine.Proposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.DeleteInput, *valueprotocolv1.DeleteOutput]
	events statemachine.Proposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput]
	get    statemachine.Querier[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.GetInput, *valueprotocolv1.GetOutput]
	watch  statemachine.Querier[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.WatchInput, *valueprotocolv1.WatchOutput]
}

func (s *ValueExecutor) init() {
	s.set = statemachine.NewProposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.SetInput, *valueprotocolv1.SetOutput]("Set").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.SetInput, bool) {
			if put, ok := input.Input.(*valueprotocolv1.ValueInput_Set); ok {
				return put.Set, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.SetOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Set{
					Set: output,
				},
			}
		}).
		Build(s.Set)
	s.insert = statemachine.NewProposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.InsertInput, *valueprotocolv1.InsertOutput]("Insert").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.InsertInput, bool) {
			if put, ok := input.Input.(*valueprotocolv1.ValueInput_Insert); ok {
				return put.Insert, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.InsertOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Insert{
					Insert: output,
				},
			}
		}).
		Build(s.Insert)
	s.update = statemachine.NewProposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.UpdateInput, *valueprotocolv1.UpdateOutput]("Update").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.UpdateInput, bool) {
			if update, ok := input.Input.(*valueprotocolv1.ValueInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.UpdateOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.delete = statemachine.NewProposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.DeleteInput, *valueprotocolv1.DeleteOutput]("Delete").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.DeleteInput, bool) {
			if remove, ok := input.Input.(*valueprotocolv1.ValueInput_Delete); ok {
				return remove.Delete, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.DeleteOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Delete{
					Delete: output,
				},
			}
		}).
		Build(s.Delete)
	s.events = statemachine.NewProposer[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.EventsInput, *valueprotocolv1.EventsOutput]("Events").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.EventsInput, bool) {
			if events, ok := input.Input.(*valueprotocolv1.ValueInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.EventsOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.get = statemachine.NewQuerier[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.GetInput, *valueprotocolv1.GetOutput]("Get").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.GetInput, bool) {
			if get, ok := input.Input.(*valueprotocolv1.ValueInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.GetOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.watch = statemachine.NewQuerier[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput, *valueprotocolv1.WatchInput, *valueprotocolv1.WatchOutput]("Watch").
		Decoder(func(input *valueprotocolv1.ValueInput) (*valueprotocolv1.WatchInput, bool) {
			if entries, ok := input.Input.(*valueprotocolv1.ValueInput_Watch); ok {
				return entries.Watch, true
			}
			return nil, false
		}).
		Encoder(func(output *valueprotocolv1.WatchOutput) *valueprotocolv1.ValueOutput {
			return &valueprotocolv1.ValueOutput{
				Output: &valueprotocolv1.ValueOutput_Watch{
					Watch: output,
				},
			}
		}).
		Build(s.Watch)
}

func (s *ValueExecutor) Propose(proposal statemachine.Proposal[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]) {
	switch proposal.Input().Input.(type) {
	case *valueprotocolv1.ValueInput_Set:
		s.set(proposal)
	case *valueprotocolv1.ValueInput_Insert:
		s.insert(proposal)
	case *valueprotocolv1.ValueInput_Update:
		s.update(proposal)
	case *valueprotocolv1.ValueInput_Delete:
		s.delete(proposal)
	case *valueprotocolv1.ValueInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *ValueExecutor) Query(query statemachine.Query[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]) {
	switch query.Input().Input.(type) {
	case *valueprotocolv1.ValueInput_Get:
		s.get(query)
	case *valueprotocolv1.ValueInput_Watch:
		s.watch(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
