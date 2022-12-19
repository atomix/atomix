// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/lock/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var lockCodec = statemachine.NewCodec[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput](
	func(bytes []byte) (*lockprotocolv1.LockInput, error) {
		input := &lockprotocolv1.LockInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *lockprotocolv1.LockOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm LockStateMachine) statemachine.Executor[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput] {
	executor := &LockExecutor{
		LockStateMachine: sm,
	}
	executor.init()
	return executor
}

type LockExecutor struct {
	LockStateMachine
	acquire statemachine.Proposer[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]
	release statemachine.Proposer[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.ReleaseInput, *lockprotocolv1.ReleaseOutput]
	get     statemachine.Querier[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.GetInput, *lockprotocolv1.GetOutput]
}

func (s *LockExecutor) init() {
	s.acquire = statemachine.NewProposer[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]("Acquire").
		Decoder(func(input *lockprotocolv1.LockInput) (*lockprotocolv1.AcquireInput, bool) {
			if set, ok := input.Input.(*lockprotocolv1.LockInput_Acquire); ok {
				return set.Acquire, true
			}
			return nil, false
		}).
		Encoder(func(output *lockprotocolv1.AcquireOutput) *lockprotocolv1.LockOutput {
			return &lockprotocolv1.LockOutput{
				Output: &lockprotocolv1.LockOutput_Acquire{
					Acquire: output,
				},
			}
		}).
		Build(s.Acquire)
	s.release = statemachine.NewProposer[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.ReleaseInput, *lockprotocolv1.ReleaseOutput]("Release").
		Decoder(func(input *lockprotocolv1.LockInput) (*lockprotocolv1.ReleaseInput, bool) {
			if set, ok := input.Input.(*lockprotocolv1.LockInput_Release); ok {
				return set.Release, true
			}
			return nil, false
		}).
		Encoder(func(output *lockprotocolv1.ReleaseOutput) *lockprotocolv1.LockOutput {
			return &lockprotocolv1.LockOutput{
				Output: &lockprotocolv1.LockOutput_Release{
					Release: output,
				},
			}
		}).
		Build(s.Release)
	s.get = statemachine.NewQuerier[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.GetInput, *lockprotocolv1.GetOutput]("Get").
		Decoder(func(input *lockprotocolv1.LockInput) (*lockprotocolv1.GetInput, bool) {
			if set, ok := input.Input.(*lockprotocolv1.LockInput_Get); ok {
				return set.Get, true
			}
			return nil, false
		}).
		Encoder(func(output *lockprotocolv1.GetOutput) *lockprotocolv1.LockOutput {
			return &lockprotocolv1.LockOutput{
				Output: &lockprotocolv1.LockOutput_Get{
					Get: output,
				},
			}
		}).
		Build(s.Get)
}

func (s *LockExecutor) Propose(proposal statemachine.Proposal[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]) {
	switch proposal.Input().Input.(type) {
	case *lockprotocolv1.LockInput_Acquire:
		s.acquire(proposal)
	case *lockprotocolv1.LockInput_Release:
		s.release(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
	}
}

func (s *LockExecutor) Query(query statemachine.Query[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]) {
	switch query.Input().Input.(type) {
	case *lockprotocolv1.LockInput_Get:
		s.get(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
