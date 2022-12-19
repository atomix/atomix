// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	multimapprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/multimap/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var multiMapCodec = statemachine.NewCodec[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput](
	func(bytes []byte) (*multimapprotocolv1.MultiMapInput, error) {
		input := &multimapprotocolv1.MultiMapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *multimapprotocolv1.MultiMapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm MultiMapStateMachine) statemachine.Executor[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput] {
	exector := &MultiMapExecutor{
		MultiMapStateMachine: sm,
	}
	exector.init()
	return exector
}

type MultiMapExecutor struct {
	MultiMapStateMachine
	put           statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.PutInput, *multimapprotocolv1.PutOutput]
	putAll        statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.PutAllInput, *multimapprotocolv1.PutAllOutput]
	putEntries    statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.PutEntriesInput, *multimapprotocolv1.PutEntriesOutput]
	replace       statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.ReplaceInput, *multimapprotocolv1.ReplaceOutput]
	remove        statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.RemoveInput, *multimapprotocolv1.RemoveOutput]
	removeAll     statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.RemoveAllInput, *multimapprotocolv1.RemoveAllOutput]
	removeEntries statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.RemoveEntriesInput, *multimapprotocolv1.RemoveEntriesOutput]
	clear         statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.ClearInput, *multimapprotocolv1.ClearOutput]
	events        statemachine.Proposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput]
	size          statemachine.Querier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.SizeInput, *multimapprotocolv1.SizeOutput]
	contains      statemachine.Querier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.ContainsInput, *multimapprotocolv1.ContainsOutput]
	get           statemachine.Querier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.GetInput, *multimapprotocolv1.GetOutput]
	list          statemachine.Querier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.EntriesInput, *multimapprotocolv1.EntriesOutput]
}

func (s *MultiMapExecutor) init() {
	s.put = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.PutInput, *multimapprotocolv1.PutOutput]("Put").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.PutInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Put); ok {
				return put.Put, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.PutOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Put{
					Put: output,
				},
			}
		}).
		Build(s.Put)
	s.putAll = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.PutAllInput, *multimapprotocolv1.PutAllOutput]("PutAll").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.PutAllInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_PutAll); ok {
				return put.PutAll, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.PutAllOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_PutAll{
					PutAll: output,
				},
			}
		}).
		Build(s.PutAll)
	s.putEntries = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.PutEntriesInput, *multimapprotocolv1.PutEntriesOutput]("PutEntries").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.PutEntriesInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_PutEntries); ok {
				return put.PutEntries, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.PutEntriesOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_PutEntries{
					PutEntries: output,
				},
			}
		}).
		Build(s.PutEntries)
	s.replace = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.ReplaceInput, *multimapprotocolv1.ReplaceOutput]("Replace").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.ReplaceInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Replace); ok {
				return put.Replace, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.ReplaceOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Replace{
					Replace: output,
				},
			}
		}).
		Build(s.Replace)
	s.remove = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.RemoveInput, *multimapprotocolv1.RemoveOutput]("Remove").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.RemoveInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Remove); ok {
				return put.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.RemoveOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.removeAll = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.RemoveAllInput, *multimapprotocolv1.RemoveAllOutput]("RemoveAll").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.RemoveAllInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_RemoveAll); ok {
				return put.RemoveAll, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.RemoveAllOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_RemoveAll{
					RemoveAll: output,
				},
			}
		}).
		Build(s.RemoveAll)
	s.removeEntries = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.RemoveEntriesInput, *multimapprotocolv1.RemoveEntriesOutput]("RemoveEntries").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.RemoveEntriesInput, bool) {
			if put, ok := input.Input.(*multimapprotocolv1.MultiMapInput_RemoveEntries); ok {
				return put.RemoveEntries, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.RemoveEntriesOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_RemoveEntries{
					RemoveEntries: output,
				},
			}
		}).
		Build(s.RemoveEntries)
	s.clear = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.ClearInput, *multimapprotocolv1.ClearOutput]("Clear").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.ClearInput, bool) {
			if clear, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.ClearOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.EventsInput, *multimapprotocolv1.EventsOutput]("Events").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.EventsInput, bool) {
			if events, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.EventsOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.SizeInput, *multimapprotocolv1.SizeOutput]("Size").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.SizeInput, bool) {
			if size, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.SizeOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.contains = statemachine.NewQuerier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.ContainsInput, *multimapprotocolv1.ContainsOutput]("Contains").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.ContainsInput, bool) {
			if get, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Contains); ok {
				return get.Contains, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.ContainsOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Contains{
					Contains: output,
				},
			}
		}).
		Build(s.Contains)
	s.get = statemachine.NewQuerier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.GetInput, *multimapprotocolv1.GetOutput]("Get").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.GetInput, bool) {
			if get, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.GetOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.list = statemachine.NewQuerier[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput, *multimapprotocolv1.EntriesInput, *multimapprotocolv1.EntriesOutput]("Entries").
		Decoder(func(input *multimapprotocolv1.MultiMapInput) (*multimapprotocolv1.EntriesInput, bool) {
			if entries, ok := input.Input.(*multimapprotocolv1.MultiMapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *multimapprotocolv1.EntriesOutput) *multimapprotocolv1.MultiMapOutput {
			return &multimapprotocolv1.MultiMapOutput{
				Output: &multimapprotocolv1.MultiMapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.Entries)
}

func (s *MultiMapExecutor) Propose(proposal statemachine.Proposal[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]) {
	switch proposal.Input().Input.(type) {
	case *multimapprotocolv1.MultiMapInput_Put:
		s.put(proposal)
	case *multimapprotocolv1.MultiMapInput_PutAll:
		s.putAll(proposal)
	case *multimapprotocolv1.MultiMapInput_PutEntries:
		s.putEntries(proposal)
	case *multimapprotocolv1.MultiMapInput_Replace:
		s.replace(proposal)
	case *multimapprotocolv1.MultiMapInput_Remove:
		s.remove(proposal)
	case *multimapprotocolv1.MultiMapInput_RemoveAll:
		s.removeAll(proposal)
	case *multimapprotocolv1.MultiMapInput_RemoveEntries:
		s.removeEntries(proposal)
	case *multimapprotocolv1.MultiMapInput_Clear:
		s.clear(proposal)
	case *multimapprotocolv1.MultiMapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *MultiMapExecutor) Query(query statemachine.Query[*multimapprotocolv1.MultiMapInput, *multimapprotocolv1.MultiMapOutput]) {
	switch query.Input().Input.(type) {
	case *multimapprotocolv1.MultiMapInput_Size_:
		s.size(query)
	case *multimapprotocolv1.MultiMapInput_Contains:
		s.contains(query)
	case *multimapprotocolv1.MultiMapInput_Get:
		s.get(query)
	case *multimapprotocolv1.MultiMapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
