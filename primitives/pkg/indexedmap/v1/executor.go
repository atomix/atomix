// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var indexedMapCodec = statemachine.NewCodec[*IndexedMapInput, *IndexedMapOutput](
	func(bytes []byte) (*IndexedMapInput, error) {
		input := &IndexedMapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *IndexedMapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm IndexedMapStateMachine) statemachine.Executor[*IndexedMapInput, *IndexedMapOutput] {
	executor := &IndexedMapExecutor{
		IndexedMapStateMachine: sm,
	}
	executor.init()
	return executor
}

type IndexedMapExecutor struct {
	IndexedMapStateMachine
	append statemachine.Proposer[*IndexedMapInput, *IndexedMapOutput, *AppendInput, *AppendOutput]
	update statemachine.Proposer[*IndexedMapInput, *IndexedMapOutput, *UpdateInput, *UpdateOutput]
	remove statemachine.Proposer[*IndexedMapInput, *IndexedMapOutput, *RemoveInput, *RemoveOutput]
	clear  statemachine.Proposer[*IndexedMapInput, *IndexedMapOutput, *ClearInput, *ClearOutput]
	events statemachine.Proposer[*IndexedMapInput, *IndexedMapOutput, *EventsInput, *EventsOutput]
	size   statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *SizeInput, *SizeOutput]
	get    statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *GetInput, *GetOutput]
	first  statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *FirstEntryInput, *FirstEntryOutput]
	last   statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *LastEntryInput, *LastEntryOutput]
	next   statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *NextEntryInput, *NextEntryOutput]
	prev   statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *PrevEntryInput, *PrevEntryOutput]
	list   statemachine.Querier[*IndexedMapInput, *IndexedMapOutput, *EntriesInput, *EntriesOutput]
}

func (s *IndexedMapExecutor) init() {
	s.append = statemachine.NewProposer[*IndexedMapInput, *IndexedMapOutput, *AppendInput, *AppendOutput]("Append").
		Decoder(func(input *IndexedMapInput) (*AppendInput, bool) {
			if put, ok := input.Input.(*IndexedMapInput_Append); ok {
				return put.Append, true
			}
			return nil, false
		}).
		Encoder(func(output *AppendOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Append{
					Append: output,
				},
			}
		}).
		Build(s.Append)
	s.update = statemachine.NewProposer[*IndexedMapInput, *IndexedMapOutput, *UpdateInput, *UpdateOutput]("Update").
		Decoder(func(input *IndexedMapInput) (*UpdateInput, bool) {
			if update, ok := input.Input.(*IndexedMapInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *UpdateOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.remove = statemachine.NewProposer[*IndexedMapInput, *IndexedMapOutput, *RemoveInput, *RemoveOutput]("Remove").
		Decoder(func(input *IndexedMapInput) (*RemoveInput, bool) {
			if remove, ok := input.Input.(*IndexedMapInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *RemoveOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.clear = statemachine.NewProposer[*IndexedMapInput, *IndexedMapOutput, *ClearInput, *ClearOutput]("Clear").
		Decoder(func(input *IndexedMapInput) (*ClearInput, bool) {
			if clear, ok := input.Input.(*IndexedMapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *ClearOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*IndexedMapInput, *IndexedMapOutput, *EventsInput, *EventsOutput]("Events").
		Decoder(func(input *IndexedMapInput) (*EventsInput, bool) {
			if events, ok := input.Input.(*IndexedMapInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *EventsOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *SizeInput, *SizeOutput]("Size").
		Decoder(func(input *IndexedMapInput) (*SizeInput, bool) {
			if size, ok := input.Input.(*IndexedMapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *SizeOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.get = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *GetInput, *GetOutput]("Get").
		Decoder(func(input *IndexedMapInput) (*GetInput, bool) {
			if get, ok := input.Input.(*IndexedMapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.first = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *FirstEntryInput, *FirstEntryOutput]("FirstEntry").
		Decoder(func(input *IndexedMapInput) (*FirstEntryInput, bool) {
			if get, ok := input.Input.(*IndexedMapInput_FirstEntry); ok {
				return get.FirstEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *FirstEntryOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_FirstEntry{
					FirstEntry: output,
				},
			}
		}).
		Build(s.FirstEntry)
	s.last = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *LastEntryInput, *LastEntryOutput]("LastEntry").
		Decoder(func(input *IndexedMapInput) (*LastEntryInput, bool) {
			if get, ok := input.Input.(*IndexedMapInput_LastEntry); ok {
				return get.LastEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *LastEntryOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_LastEntry{
					LastEntry: output,
				},
			}
		}).
		Build(s.LastEntry)
	s.next = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *NextEntryInput, *NextEntryOutput]("NextEntry").
		Decoder(func(input *IndexedMapInput) (*NextEntryInput, bool) {
			if get, ok := input.Input.(*IndexedMapInput_NextEntry); ok {
				return get.NextEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *NextEntryOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_NextEntry{
					NextEntry: output,
				},
			}
		}).
		Build(s.NextEntry)
	s.prev = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *PrevEntryInput, *PrevEntryOutput]("PrevEntry").
		Decoder(func(input *IndexedMapInput) (*PrevEntryInput, bool) {
			if get, ok := input.Input.(*IndexedMapInput_PrevEntry); ok {
				return get.PrevEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *PrevEntryOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_PrevEntry{
					PrevEntry: output,
				},
			}
		}).
		Build(s.PrevEntry)
	s.list = statemachine.NewQuerier[*IndexedMapInput, *IndexedMapOutput, *EntriesInput, *EntriesOutput]("Entries").
		Decoder(func(input *IndexedMapInput) (*EntriesInput, bool) {
			if entries, ok := input.Input.(*IndexedMapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *EntriesOutput) *IndexedMapOutput {
			return &IndexedMapOutput{
				Output: &IndexedMapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.Entries)
}

func (s *IndexedMapExecutor) Propose(proposal statemachine.Proposal[*IndexedMapInput, *IndexedMapOutput]) {
	switch proposal.Input().Input.(type) {
	case *IndexedMapInput_Append:
		s.append(proposal)
	case *IndexedMapInput_Update:
		s.update(proposal)
	case *IndexedMapInput_Remove:
		s.remove(proposal)
	case *IndexedMapInput_Clear:
		s.clear(proposal)
	case *IndexedMapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *IndexedMapExecutor) Query(query statemachine.Query[*IndexedMapInput, *IndexedMapOutput]) {
	switch query.Input().Input.(type) {
	case *IndexedMapInput_Size_:
		s.size(query)
	case *IndexedMapInput_Get:
		s.get(query)
	case *IndexedMapInput_FirstEntry:
		s.first(query)
	case *IndexedMapInput_LastEntry:
		s.last(query)
	case *IndexedMapInput_NextEntry:
		s.next(query)
	case *IndexedMapInput_PrevEntry:
		s.prev(query)
	case *IndexedMapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
