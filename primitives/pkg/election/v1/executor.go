// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/gogo/protobuf/proto"
)

var lockCodec = statemachine.NewCodec[*LeaderElectionInput, *LeaderElectionOutput](
	func(bytes []byte) (*LeaderElectionInput, error) {
		input := &LeaderElectionInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *LeaderElectionOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm LeaderElectionStateMachine) statemachine.Executor[*LeaderElectionInput, *LeaderElectionOutput] {
	executor := &LeaderElectionExecutor{
		LeaderElectionStateMachine: sm,
	}
	executor.init()
	return executor
}

type LeaderElectionExecutor struct {
	LeaderElectionStateMachine
	enter    statemachine.Proposer[*LeaderElectionInput, *LeaderElectionOutput, *EnterInput, *EnterOutput]
	withdraw statemachine.Proposer[*LeaderElectionInput, *LeaderElectionOutput, *WithdrawInput, *WithdrawOutput]
	anoint   statemachine.Proposer[*LeaderElectionInput, *LeaderElectionOutput, *AnointInput, *AnointOutput]
	promote  statemachine.Proposer[*LeaderElectionInput, *LeaderElectionOutput, *PromoteInput, *PromoteOutput]
	demote   statemachine.Proposer[*LeaderElectionInput, *LeaderElectionOutput, *DemoteInput, *DemoteOutput]
	evict    statemachine.Proposer[*LeaderElectionInput, *LeaderElectionOutput, *EvictInput, *EvictOutput]
	getTerm  statemachine.Querier[*LeaderElectionInput, *LeaderElectionOutput, *GetTermInput, *GetTermOutput]
	watch    statemachine.Querier[*LeaderElectionInput, *LeaderElectionOutput, *WatchInput, *WatchOutput]
}

func (s *LeaderElectionExecutor) init() {
	s.enter = statemachine.NewProposer[*LeaderElectionInput, *LeaderElectionOutput, *EnterInput, *EnterOutput]("Enter").
		Decoder(func(input *LeaderElectionInput) (*EnterInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Enter); ok {
				return set.Enter, true
			}
			return nil, false
		}).
		Encoder(func(output *EnterOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Enter{
					Enter: output,
				},
			}
		}).
		Build(s.Enter)
	s.withdraw = statemachine.NewProposer[*LeaderElectionInput, *LeaderElectionOutput, *WithdrawInput, *WithdrawOutput]("Withdraw").
		Decoder(func(input *LeaderElectionInput) (*WithdrawInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Withdraw); ok {
				return set.Withdraw, true
			}
			return nil, false
		}).
		Encoder(func(output *WithdrawOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Withdraw{
					Withdraw: output,
				},
			}
		}).
		Build(s.Withdraw)
	s.anoint = statemachine.NewProposer[*LeaderElectionInput, *LeaderElectionOutput, *AnointInput, *AnointOutput]("Anoint").
		Decoder(func(input *LeaderElectionInput) (*AnointInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Anoint); ok {
				return set.Anoint, true
			}
			return nil, false
		}).
		Encoder(func(output *AnointOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Anoint{
					Anoint: output,
				},
			}
		}).
		Build(s.Anoint)
	s.promote = statemachine.NewProposer[*LeaderElectionInput, *LeaderElectionOutput, *PromoteInput, *PromoteOutput]("Promote").
		Decoder(func(input *LeaderElectionInput) (*PromoteInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Promote); ok {
				return set.Promote, true
			}
			return nil, false
		}).
		Encoder(func(output *PromoteOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Promote{
					Promote: output,
				},
			}
		}).
		Build(s.Promote)
	s.demote = statemachine.NewProposer[*LeaderElectionInput, *LeaderElectionOutput, *DemoteInput, *DemoteOutput]("Demote").
		Decoder(func(input *LeaderElectionInput) (*DemoteInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Demote); ok {
				return set.Demote, true
			}
			return nil, false
		}).
		Encoder(func(output *DemoteOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Demote{
					Demote: output,
				},
			}
		}).
		Build(s.Demote)
	s.evict = statemachine.NewProposer[*LeaderElectionInput, *LeaderElectionOutput, *EvictInput, *EvictOutput]("Evict").
		Decoder(func(input *LeaderElectionInput) (*EvictInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Evict); ok {
				return set.Evict, true
			}
			return nil, false
		}).
		Encoder(func(output *EvictOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Evict{
					Evict: output,
				},
			}
		}).
		Build(s.Evict)
	s.getTerm = statemachine.NewQuerier[*LeaderElectionInput, *LeaderElectionOutput, *GetTermInput, *GetTermOutput]("GetTerm").
		Decoder(func(input *LeaderElectionInput) (*GetTermInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_GetTerm); ok {
				return set.GetTerm, true
			}
			return nil, false
		}).
		Encoder(func(output *GetTermOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_GetTerm{
					GetTerm: output,
				},
			}
		}).
		Build(s.GetTerm)
	s.watch = statemachine.NewQuerier[*LeaderElectionInput, *LeaderElectionOutput, *WatchInput, *WatchOutput]("Watch").
		Decoder(func(input *LeaderElectionInput) (*WatchInput, bool) {
			if set, ok := input.Input.(*LeaderElectionInput_Watch); ok {
				return set.Watch, true
			}
			return nil, false
		}).
		Encoder(func(output *WatchOutput) *LeaderElectionOutput {
			return &LeaderElectionOutput{
				Output: &LeaderElectionOutput_Watch{
					Watch: output,
				},
			}
		}).
		Build(s.Watch)
}

func (s *LeaderElectionExecutor) Propose(proposal statemachine.Proposal[*LeaderElectionInput, *LeaderElectionOutput]) {
	switch proposal.Input().Input.(type) {
	case *LeaderElectionInput_Enter:
		s.enter(proposal)
	case *LeaderElectionInput_Withdraw:
		s.withdraw(proposal)
	case *LeaderElectionInput_Anoint:
		s.anoint(proposal)
	case *LeaderElectionInput_Promote:
		s.promote(proposal)
	case *LeaderElectionInput_Demote:
		s.demote(proposal)
	case *LeaderElectionInput_Evict:
		s.evict(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
	}
}

func (s *LeaderElectionExecutor) Query(query statemachine.Query[*LeaderElectionInput, *LeaderElectionOutput]) {
	switch query.Input().Input.(type) {
	case *LeaderElectionInput_GetTerm:
		s.getTerm(query)
	case *LeaderElectionInput_Watch:
		s.watch(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
