// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var lockCodec = statemachine.NewCodec[*LockInput, *LockOutput](
	func(bytes []byte) (*LockInput, error) {
		input := &LockInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *LockOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm LockStateMachine) statemachine.Executor[*LockInput, *LockOutput] {
	executor := &LockExecutor{
		LockStateMachine: sm,
	}
	executor.init()
	return executor
}

type LockExecutor struct {
	LockStateMachine
	acquire statemachine.Proposer[*LockInput, *LockOutput, *AcquireInput, *AcquireOutput]
	release statemachine.Proposer[*LockInput, *LockOutput, *ReleaseInput, *ReleaseOutput]
	get     statemachine.Querier[*LockInput, *LockOutput, *GetInput, *GetOutput]
}

func (s *LockExecutor) init() {
	s.acquire = statemachine.NewProposer[*LockInput, *LockOutput, *AcquireInput, *AcquireOutput]("Acquire").
		Decoder(func(input *LockInput) (*AcquireInput, bool) {
			if set, ok := input.Input.(*LockInput_Acquire); ok {
				return set.Acquire, true
			}
			return nil, false
		}).
		Encoder(func(output *AcquireOutput) *LockOutput {
			return &LockOutput{
				Output: &LockOutput_Acquire{
					Acquire: output,
				},
			}
		}).
		Build(s.Acquire)
	s.release = statemachine.NewProposer[*LockInput, *LockOutput, *ReleaseInput, *ReleaseOutput]("Release").
		Decoder(func(input *LockInput) (*ReleaseInput, bool) {
			if set, ok := input.Input.(*LockInput_Release); ok {
				return set.Release, true
			}
			return nil, false
		}).
		Encoder(func(output *ReleaseOutput) *LockOutput {
			return &LockOutput{
				Output: &LockOutput_Release{
					Release: output,
				},
			}
		}).
		Build(s.Release)
	s.get = statemachine.NewQuerier[*LockInput, *LockOutput, *GetInput, *GetOutput]("Get").
		Decoder(func(input *LockInput) (*GetInput, bool) {
			if set, ok := input.Input.(*LockInput_Get); ok {
				return set.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *GetOutput) *LockOutput {
			return &LockOutput{
				Output: &LockOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
}

func (s *LockExecutor) Propose(proposal statemachine.Proposal[*LockInput, *LockOutput]) {
	switch proposal.Input().Input.(type) {
	case *LockInput_Acquire:
		s.acquire(proposal)
	case *LockInput_Release:
		s.release(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
	}
}

func (s *LockExecutor) Query(query statemachine.Query[*LockInput, *LockOutput]) {
	switch query.Input().Input.(type) {
	case *LockInput_Get:
		s.get(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
