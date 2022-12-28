// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/election/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
)

var lockCodec = statemachine.NewCodec[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput](
	func(bytes []byte) (*electionprotocolv1.LeaderElectionInput, error) {
		input := &electionprotocolv1.LeaderElectionInput{}
		if err := proto.Unmarshal(bytes, input); err != nil {
			return nil, err
		}
		return input, nil
	},
	func(output *electionprotocolv1.LeaderElectionOutput) ([]byte, error) {
		return proto.Marshal(output)
	})

func newExecutor(sm LeaderElectionStateMachine) statemachine.PrimitiveStateMachine[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput] {
	executor := &LeaderElectionExecutor{
		LeaderElectionStateMachine: sm,
	}
	executor.init()
	return executor
}

type LeaderElectionExecutor struct {
	LeaderElectionStateMachine
	enter    statemachine.Proposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.EnterInput, *electionprotocolv1.EnterOutput]
	withdraw statemachine.Proposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.WithdrawInput, *electionprotocolv1.WithdrawOutput]
	anoint   statemachine.Proposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.AnointInput, *electionprotocolv1.AnointOutput]
	promote  statemachine.Proposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.PromoteInput, *electionprotocolv1.PromoteOutput]
	demote   statemachine.Proposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.DemoteInput, *electionprotocolv1.DemoteOutput]
	evict    statemachine.Proposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.EvictInput, *electionprotocolv1.EvictOutput]
	getTerm  statemachine.Querier[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.GetTermInput, *electionprotocolv1.GetTermOutput]
	watch    statemachine.Querier[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.WatchInput, *electionprotocolv1.WatchOutput]
}

func (s *LeaderElectionExecutor) init() {
	s.enter = statemachine.NewProposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.EnterInput, *electionprotocolv1.EnterOutput]("Enter").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.EnterInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Enter); ok {
				return set.Enter, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.EnterOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Enter{
					Enter: output,
				},
			}
		}).
		Build(s.Enter)
	s.withdraw = statemachine.NewProposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.WithdrawInput, *electionprotocolv1.WithdrawOutput]("Withdraw").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.WithdrawInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Withdraw); ok {
				return set.Withdraw, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.WithdrawOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Withdraw{
					Withdraw: output,
				},
			}
		}).
		Build(s.Withdraw)
	s.anoint = statemachine.NewProposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.AnointInput, *electionprotocolv1.AnointOutput]("Anoint").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.AnointInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Anoint); ok {
				return set.Anoint, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.AnointOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Anoint{
					Anoint: output,
				},
			}
		}).
		Build(s.Anoint)
	s.promote = statemachine.NewProposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.PromoteInput, *electionprotocolv1.PromoteOutput]("Promote").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.PromoteInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Promote); ok {
				return set.Promote, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.PromoteOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Promote{
					Promote: output,
				},
			}
		}).
		Build(s.Promote)
	s.demote = statemachine.NewProposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.DemoteInput, *electionprotocolv1.DemoteOutput]("Demote").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.DemoteInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Demote); ok {
				return set.Demote, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.DemoteOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Demote{
					Demote: output,
				},
			}
		}).
		Build(s.Demote)
	s.evict = statemachine.NewProposer[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.EvictInput, *electionprotocolv1.EvictOutput]("Evict").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.EvictInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Evict); ok {
				return set.Evict, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.EvictOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Evict{
					Evict: output,
				},
			}
		}).
		Build(s.Evict)
	s.getTerm = statemachine.NewQuerier[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.GetTermInput, *electionprotocolv1.GetTermOutput]("GetTerm").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.GetTermInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_GetTerm); ok {
				return set.GetTerm, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.GetTermOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_GetTerm{
					GetTerm: output,
				},
			}
		}).
		Build(s.GetTerm)
	s.watch = statemachine.NewQuerier[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput, *electionprotocolv1.WatchInput, *electionprotocolv1.WatchOutput]("Watch").
		Decoder(func(input *electionprotocolv1.LeaderElectionInput) (*electionprotocolv1.WatchInput, bool) {
			if set, ok := input.Input.(*electionprotocolv1.LeaderElectionInput_Watch); ok {
				return set.Watch, true
			}
			return nil, false
		}).
		Encoder(func(output *electionprotocolv1.WatchOutput) *electionprotocolv1.LeaderElectionOutput {
			return &electionprotocolv1.LeaderElectionOutput{
				Output: &electionprotocolv1.LeaderElectionOutput_Watch{
					Watch: output,
				},
			}
		}).
		Build(s.Watch)
}

func (s *LeaderElectionExecutor) Propose(proposal statemachine.Proposal[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]) {
	switch proposal.Input().Input.(type) {
	case *electionprotocolv1.LeaderElectionInput_Enter:
		s.enter(proposal)
	case *electionprotocolv1.LeaderElectionInput_Withdraw:
		s.withdraw(proposal)
	case *electionprotocolv1.LeaderElectionInput_Anoint:
		s.anoint(proposal)
	case *electionprotocolv1.LeaderElectionInput_Promote:
		s.promote(proposal)
	case *electionprotocolv1.LeaderElectionInput_Demote:
		s.demote(proposal)
	case *electionprotocolv1.LeaderElectionInput_Evict:
		s.evict(proposal)
	default:
		proposal.Error(errors.NewNotSupported("proposal not supported"))
	}
}

func (s *LeaderElectionExecutor) Query(query statemachine.Query[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]) {
	switch query.Input().Input.(type) {
	case *electionprotocolv1.LeaderElectionInput_GetTerm:
		s.getTerm(query)
	case *electionprotocolv1.LeaderElectionInput_Watch:
		s.watch(query)
	default:
		query.Error(errors.NewNotSupported("query not supported"))
	}
}
