// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var multiMapCodec = statemachine.NewCodec[*MultiMapInput, *MultiMapOutput](
	func(bytes []byte) (*MultiMapInput, error) {
		input := &MultiMapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *MultiMapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm MultiMapStateMachine) statemachine.Executor[*MultiMapInput, *MultiMapOutput] {
	exector := &MultiMapExecutor{
		MultiMapStateMachine: sm,
	}
	exector.init()
	return exector
}

type MultiMapExecutor struct {
	MultiMapStateMachine
	put           statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *PutInput, *PutOutput]
	putAll        statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *PutAllInput, *PutAllOutput]
	putEntries    statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *PutEntriesInput, *PutEntriesOutput]
	replace       statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *ReplaceInput, *ReplaceOutput]
	remove        statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *RemoveInput, *RemoveOutput]
	removeAll     statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *RemoveAllInput, *RemoveAllOutput]
	removeEntries statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *RemoveEntriesInput, *RemoveEntriesOutput]
	clear         statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *ClearInput, *ClearOutput]
	events        statemachine.Proposer[*MultiMapInput, *MultiMapOutput, *EventsInput, *EventsOutput]
	size          statemachine.Querier[*MultiMapInput, *MultiMapOutput, *SizeInput, *SizeOutput]
	contains      statemachine.Querier[*MultiMapInput, *MultiMapOutput, *ContainsInput, *ContainsOutput]
	get           statemachine.Querier[*MultiMapInput, *MultiMapOutput, *GetInput, *GetOutput]
	list          statemachine.Querier[*MultiMapInput, *MultiMapOutput, *EntriesInput, *EntriesOutput]
}

func (s *MultiMapExecutor) init() {
	s.put = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *PutInput, *PutOutput]("Put").
		Decoder(func(input *MultiMapInput) (*PutInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_Put); ok {
				return put.Put, true
			}
			return nil, false
		}).
		Encoder(func(output *PutOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Put{
					Put: output,
				},
			}
		}).
		Build(s.Put)
	s.putAll = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *PutAllInput, *PutAllOutput]("PutAll").
		Decoder(func(input *MultiMapInput) (*PutAllInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_PutAll); ok {
				return put.PutAll, true
			}
			return nil, false
		}).
		Encoder(func(output *PutAllOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_PutAll{
					PutAll: output,
				},
			}
		}).
		Build(s.PutAll)
	s.putEntries = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *PutEntriesInput, *PutEntriesOutput]("PutEntries").
		Decoder(func(input *MultiMapInput) (*PutEntriesInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_PutEntries); ok {
				return put.PutEntries, true
			}
			return nil, false
		}).
		Encoder(func(output *PutEntriesOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_PutEntries{
					PutEntries: output,
				},
			}
		}).
		Build(s.PutEntries)
	s.replace = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *ReplaceInput, *ReplaceOutput]("Replace").
		Decoder(func(input *MultiMapInput) (*ReplaceInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_Replace); ok {
				return put.Replace, true
			}
			return nil, false
		}).
		Encoder(func(output *ReplaceOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Replace{
					Replace: output,
				},
			}
		}).
		Build(s.Replace)
	s.remove = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *RemoveInput, *RemoveOutput]("Remove").
		Decoder(func(input *MultiMapInput) (*RemoveInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_Remove); ok {
				return put.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.removeAll = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *RemoveAllInput, *RemoveAllOutput]("RemoveAll").
		Decoder(func(input *MultiMapInput) (*RemoveAllInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_RemoveAll); ok {
				return put.RemoveAll, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveAllOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_RemoveAll{
					RemoveAll: output,
				},
			}
		}).
		Build(s.RemoveAll)
	s.removeEntries = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *RemoveEntriesInput, *RemoveEntriesOutput]("RemoveEntries").
		Decoder(func(input *MultiMapInput) (*RemoveEntriesInput, bool) {
			if put, ok := input.Input.(*MultiMapInput_RemoveEntries); ok {
				return put.RemoveEntries, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveEntriesOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_RemoveEntries{
					RemoveEntries: output,
				},
			}
		}).
		Build(s.RemoveEntries)
	s.clear = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *ClearInput, *ClearOutput]("Clear").
		Decoder(func(input *MultiMapInput) (*ClearInput, bool) {
			if clear, ok := input.Input.(*MultiMapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *ClearOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*MultiMapInput, *MultiMapOutput, *EventsInput, *EventsOutput]("Events").
		Decoder(func(input *MultiMapInput) (*EventsInput, bool) {
			if events, ok := input.Input.(*MultiMapInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *EventsOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*MultiMapInput, *MultiMapOutput, *SizeInput, *SizeOutput]("Size").
		Decoder(func(input *MultiMapInput) (*SizeInput, bool) {
			if size, ok := input.Input.(*MultiMapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *SizeOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.contains = statemachine.NewQuerier[*MultiMapInput, *MultiMapOutput, *ContainsInput, *ContainsOutput]("Contains").
		Decoder(func(input *MultiMapInput) (*ContainsInput, bool) {
			if get, ok := input.Input.(*MultiMapInput_Contains); ok {
				return get.Contains, true
			}
			return nil, false
		}).
		Encoder(func(output *ContainsOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Contains{
					Contains: output,
				},
			}
		}).
		Build(s.Contains)
	s.get = statemachine.NewQuerier[*MultiMapInput, *MultiMapOutput, *GetInput, *GetOutput]("Get").
		Decoder(func(input *MultiMapInput) (*GetInput, bool) {
			if get, ok := input.Input.(*MultiMapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.list = statemachine.NewQuerier[*MultiMapInput, *MultiMapOutput, *EntriesInput, *EntriesOutput]("Entries").
		Decoder(func(input *MultiMapInput) (*EntriesInput, bool) {
			if entries, ok := input.Input.(*MultiMapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *EntriesOutput) *MultiMapOutput {
			return &MultiMapOutput{
				Output: &MultiMapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.Entries)
}

func (s *MultiMapExecutor) Propose(proposal statemachine.Proposal[*MultiMapInput, *MultiMapOutput]) {
	switch proposal.Input().Input.(type) {
	case *MultiMapInput_Put:
		s.put(proposal)
	case *MultiMapInput_PutAll:
		s.putAll(proposal)
	case *MultiMapInput_PutEntries:
		s.putEntries(proposal)
	case *MultiMapInput_Replace:
		s.replace(proposal)
	case *MultiMapInput_Remove:
		s.remove(proposal)
	case *MultiMapInput_RemoveAll:
		s.removeAll(proposal)
	case *MultiMapInput_RemoveEntries:
		s.removeEntries(proposal)
	case *MultiMapInput_Clear:
		s.clear(proposal)
	case *MultiMapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *MultiMapExecutor) Query(query statemachine.Query[*MultiMapInput, *MultiMapOutput]) {
	switch query.Input().Input.(type) {
	case *MultiMapInput_Size_:
		s.size(query)
	case *MultiMapInput_Contains:
		s.contains(query)
	case *MultiMapInput_Get:
		s.get(query)
	case *MultiMapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
