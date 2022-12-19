// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/indexedmap/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var indexedMapCodec = statemachine.NewCodec[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput](
	func(bytes []byte) (*indexedmapprotocolv1.IndexedMapInput, error) {
		input := &indexedmapprotocolv1.IndexedMapInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *indexedmapprotocolv1.IndexedMapOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm IndexedMapStateMachine) statemachine.PrimitiveStateMachine[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput] {
	executor := &IndexedMapExecutor{
		IndexedMapStateMachine: sm,
	}
	executor.init()
	return executor
}

type IndexedMapExecutor struct {
	IndexedMapStateMachine
	append statemachine.Proposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.AppendInput, *indexedmapprotocolv1.AppendOutput]
	update statemachine.Proposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.UpdateInput, *indexedmapprotocolv1.UpdateOutput]
	remove statemachine.Proposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.RemoveInput, *indexedmapprotocolv1.RemoveOutput]
	clear  statemachine.Proposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.ClearInput, *indexedmapprotocolv1.ClearOutput]
	events statemachine.Proposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput]
	size   statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.SizeInput, *indexedmapprotocolv1.SizeOutput]
	get    statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.GetInput, *indexedmapprotocolv1.GetOutput]
	first  statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.FirstEntryInput, *indexedmapprotocolv1.FirstEntryOutput]
	last   statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.LastEntryInput, *indexedmapprotocolv1.LastEntryOutput]
	next   statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.NextEntryInput, *indexedmapprotocolv1.NextEntryOutput]
	prev   statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.PrevEntryInput, *indexedmapprotocolv1.PrevEntryOutput]
	list   statemachine.Querier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.EntriesInput, *indexedmapprotocolv1.EntriesOutput]
}

func (s *IndexedMapExecutor) init() {
	s.append = statemachine.NewProposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.AppendInput, *indexedmapprotocolv1.AppendOutput]("Append").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.AppendInput, bool) {
			if put, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Append); ok {
				return put.Append, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.AppendOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Append{
					Append: output,
				},
			}
		}).
		Build(s.Append)
	s.update = statemachine.NewProposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.UpdateInput, *indexedmapprotocolv1.UpdateOutput]("Update").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.UpdateInput, bool) {
			if update, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Update); ok {
				return update.Update, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.UpdateOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Update{
					Update: output,
				},
			}
		}).
		Build(s.Update)
	s.remove = statemachine.NewProposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.RemoveInput, *indexedmapprotocolv1.RemoveOutput]("Remove").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.RemoveInput, bool) {
			if remove, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Remove); ok {
				return remove.Remove, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.RemoveOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Remove{
					Remove: output,
				},
			}
		}).
		Build(s.Remove)
	s.clear = statemachine.NewProposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.ClearInput, *indexedmapprotocolv1.ClearOutput]("Clear").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.ClearInput, bool) {
			if clear, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Clear); ok {
				return clear.Clear, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.ClearOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Clear{
					Clear: output,
				},
			}
		}).
		Build(s.Clear)
	s.events = statemachine.NewProposer[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.EventsInput, *indexedmapprotocolv1.EventsOutput]("Events").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.EventsInput, bool) {
			if events, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Events); ok {
				return events.Events, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.EventsOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Events{
					Events: output,
				},
			}
		}).
		Build(s.Events)
	s.size = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.SizeInput, *indexedmapprotocolv1.SizeOutput]("Size").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.SizeInput, bool) {
			if size, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Size_); ok {
				return size.Size_, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.SizeOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Size_{
					Size_: output,
				},
			}
		}).
		Build(s.Size)
	s.get = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.GetInput, *indexedmapprotocolv1.GetOutput]("Get").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.GetInput, bool) {
			if get, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Get); ok {
				return get.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.GetOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
	s.first = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.FirstEntryInput, *indexedmapprotocolv1.FirstEntryOutput]("FirstEntry").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.FirstEntryInput, bool) {
			if get, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_FirstEntry); ok {
				return get.FirstEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.FirstEntryOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_FirstEntry{
					FirstEntry: output,
				},
			}
		}).
		Build(s.FirstEntry)
	s.last = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.LastEntryInput, *indexedmapprotocolv1.LastEntryOutput]("LastEntry").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.LastEntryInput, bool) {
			if get, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_LastEntry); ok {
				return get.LastEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.LastEntryOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_LastEntry{
					LastEntry: output,
				},
			}
		}).
		Build(s.LastEntry)
	s.next = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.NextEntryInput, *indexedmapprotocolv1.NextEntryOutput]("NextEntry").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.NextEntryInput, bool) {
			if get, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_NextEntry); ok {
				return get.NextEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.NextEntryOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_NextEntry{
					NextEntry: output,
				},
			}
		}).
		Build(s.NextEntry)
	s.prev = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.PrevEntryInput, *indexedmapprotocolv1.PrevEntryOutput]("PrevEntry").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.PrevEntryInput, bool) {
			if get, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_PrevEntry); ok {
				return get.PrevEntry, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.PrevEntryOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_PrevEntry{
					PrevEntry: output,
				},
			}
		}).
		Build(s.PrevEntry)
	s.list = statemachine.NewQuerier[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput, *indexedmapprotocolv1.EntriesInput, *indexedmapprotocolv1.EntriesOutput]("Entries").
		Decoder(func(input *indexedmapprotocolv1.IndexedMapInput) (*indexedmapprotocolv1.EntriesInput, bool) {
			if entries, ok := input.Input.(*indexedmapprotocolv1.IndexedMapInput_Entries); ok {
				return entries.Entries, true
			}
			return nil, false
		}).
		Encoder(func(output *indexedmapprotocolv1.EntriesOutput) *indexedmapprotocolv1.IndexedMapOutput {
			return &indexedmapprotocolv1.IndexedMapOutput{
				Output: &indexedmapprotocolv1.IndexedMapOutput_Entries{
					Entries: output,
				},
			}
		}).
		Build(s.Entries)
}

func (s *IndexedMapExecutor) Propose(proposal statemachine.Proposal[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]) {
	switch proposal.Input().Input.(type) {
	case *indexedmapprotocolv1.IndexedMapInput_Append:
		s.append(proposal)
	case *indexedmapprotocolv1.IndexedMapInput_Update:
		s.update(proposal)
	case *indexedmapprotocolv1.IndexedMapInput_Remove:
		s.remove(proposal)
	case *indexedmapprotocolv1.IndexedMapInput_Clear:
		s.clear(proposal)
	case *indexedmapprotocolv1.IndexedMapInput_Events:
		s.events(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
		proposal.Close()
	}
}

func (s *IndexedMapExecutor) Query(query statemachine.Query[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]) {
	switch query.Input().Input.(type) {
	case *indexedmapprotocolv1.IndexedMapInput_Size_:
		s.size(query)
	case *indexedmapprotocolv1.IndexedMapInput_Get:
		s.get(query)
	case *indexedmapprotocolv1.IndexedMapInput_FirstEntry:
		s.first(query)
	case *indexedmapprotocolv1.IndexedMapInput_LastEntry:
		s.last(query)
	case *indexedmapprotocolv1.IndexedMapInput_NextEntry:
		s.next(query)
	case *indexedmapprotocolv1.IndexedMapInput_PrevEntry:
		s.prev(query)
	case *indexedmapprotocolv1.IndexedMapInput_Entries:
		s.list(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
