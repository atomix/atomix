// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/election/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"sync"
)

const (
	Name       = "LeaderElection"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]) statemachine.PrimitiveStateMachine[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput] {
			return newExecutor(NewLeaderElectionStateMachine(context))
		}, lockCodec)
}

type LeaderElectionContext interface {
	statemachine.PrimitiveContext[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]
}

type LeaderElectionStateMachine interface {
	statemachine.Context[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]
	statemachine.Recoverable
	Enter(statemachine.Proposal[*electionprotocolv1.EnterInput, *electionprotocolv1.EnterOutput])
	Withdraw(statemachine.Proposal[*electionprotocolv1.WithdrawInput, *electionprotocolv1.WithdrawOutput])
	Anoint(statemachine.Proposal[*electionprotocolv1.AnointInput, *electionprotocolv1.AnointOutput])
	Promote(statemachine.Proposal[*electionprotocolv1.PromoteInput, *electionprotocolv1.PromoteOutput])
	Demote(statemachine.Proposal[*electionprotocolv1.DemoteInput, *electionprotocolv1.DemoteOutput])
	Evict(statemachine.Proposal[*electionprotocolv1.EvictInput, *electionprotocolv1.EvictOutput])
	GetTerm(statemachine.Query[*electionprotocolv1.GetTermInput, *electionprotocolv1.GetTermOutput])
	Watch(statemachine.Query[*electionprotocolv1.WatchInput, *electionprotocolv1.WatchOutput])
}

func NewLeaderElectionStateMachine(context statemachine.PrimitiveContext[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]) LeaderElectionStateMachine {
	return &leaderElectionStateMachine{
		LeaderElectionContext: context,
		watchers:              make(map[statemachine.QueryID]statemachine.Query[*electionprotocolv1.WatchInput, *electionprotocolv1.WatchOutput]),
	}
}

type leaderElectionStateMachine struct {
	LeaderElectionContext
	electionprotocolv1.LeaderElectionSnapshot
	cancel   statemachine.CancelFunc
	watchers map[statemachine.QueryID]statemachine.Query[*electionprotocolv1.WatchInput, *electionprotocolv1.WatchOutput]
	mu       sync.RWMutex
}

func (s *leaderElectionStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	if err := writer.WriteMessage(&s.LeaderElectionSnapshot); err != nil {
		return err
	}
	return nil
}

func (s *leaderElectionStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	if err := reader.ReadMessage(&s.LeaderElectionSnapshot); err != nil {
		return err
	}
	if s.Leader != nil {
		if err := s.watchSession(statemachine.SessionID(s.Leader.SessionID)); err != nil {
			return err
		}
	}
	return nil
}

func (s *leaderElectionStateMachine) watchSession(sessionID statemachine.SessionID) error {
	if s.cancel != nil {
		s.cancel()
	}
	session, ok := s.Sessions().Get(sessionID)
	if !ok {
		return errors.NewFault("unknown session")
	}
	s.cancel = session.Watch(func(state statemachine.State) {
		if state == statemachine.Closed {
			s.Leader = nil
			for len(s.Candidates) > 0 {
				candidate := s.Candidates[0]
				s.Candidates = s.Candidates[1:]
				if statemachine.SessionID(candidate.SessionID) != session.ID() {
					s.Leader = &candidate
					s.Term++
					s.notify(s.term())
					break
				}
			}
		}
	})
	return nil
}

func (s *leaderElectionStateMachine) term() electionprotocolv1.Term {
	term := electionprotocolv1.Term{
		Index: s.Term,
	}
	if s.Leader != nil {
		term.Leader = s.Leader.Name
	}
	for _, candidate := range s.Candidates {
		term.Candidates = append(term.Candidates, candidate.Name)
	}
	return term
}

func (s *leaderElectionStateMachine) candidateExists(name string) bool {
	for _, candidate := range s.Candidates {
		if candidate.Name == name {
			return true
		}
	}
	return false
}

func (s *leaderElectionStateMachine) Enter(proposal statemachine.Proposal[*electionprotocolv1.EnterInput, *electionprotocolv1.EnterOutput]) {
	defer proposal.Close()
	if s.Leader == nil {
		s.Term++
		s.Leader = &electionprotocolv1.LeaderElectionCandidate{
			Name:      proposal.Input().Candidate,
			SessionID: protocol.SessionID(proposal.Session().ID()),
		}
		if err := s.watchSession(proposal.Session().ID()); err != nil {
			panic(err)
		}
		term := s.term()
		s.notify(term)
		proposal.Output(&electionprotocolv1.EnterOutput{
			Term: term,
		})
	} else if !s.candidateExists(proposal.Input().Candidate) {
		s.Candidates = append(s.Candidates, electionprotocolv1.LeaderElectionCandidate{
			Name:      proposal.Input().Candidate,
			SessionID: protocol.SessionID(proposal.Session().ID()),
		})
		term := s.term()
		s.notify(term)
		proposal.Output(&electionprotocolv1.EnterOutput{
			Term: term,
		})
	} else {
		proposal.Output(&electionprotocolv1.EnterOutput{
			Term: s.term(),
		})
	}
}

func (s *leaderElectionStateMachine) Withdraw(proposal statemachine.Proposal[*electionprotocolv1.WithdrawInput, *electionprotocolv1.WithdrawOutput]) {
	defer proposal.Close()
	if s.Leader != nil && statemachine.SessionID(s.Leader.SessionID) == proposal.Session().ID() {
		s.Leader = nil
		if len(s.Candidates) > 0 {
			candidate := s.Candidates[0]
			s.Candidates = s.Candidates[1:]
			s.Leader = &candidate
			s.Term++
		}
		term := s.term()
		s.notify(term)
		proposal.Output(&electionprotocolv1.WithdrawOutput{
			Term: term,
		})
	} else {
		proposal.Output(&electionprotocolv1.WithdrawOutput{
			Term: s.term(),
		})
	}
}

func (s *leaderElectionStateMachine) Anoint(proposal statemachine.Proposal[*electionprotocolv1.AnointInput, *electionprotocolv1.AnointOutput]) {
	defer proposal.Close()
	if s.Leader == nil || s.Leader.Name == proposal.Input().Candidate || !s.candidateExists(proposal.Input().Candidate) {
		proposal.Output(&electionprotocolv1.AnointOutput{
			Term: s.term(),
		})
		return
	}

	candidates := make([]electionprotocolv1.LeaderElectionCandidate, 0, len(s.Candidates))
	for _, candidate := range s.Candidates {
		if candidate.Name == proposal.Input().Candidate {
			s.Term++
			s.Leader = &candidate
		} else {
			candidates = append(candidates, candidate)
		}
	}
	s.Candidates = candidates

	term := s.term()
	s.notify(term)
	proposal.Output(&electionprotocolv1.AnointOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) Promote(proposal statemachine.Proposal[*electionprotocolv1.PromoteInput, *electionprotocolv1.PromoteOutput]) {
	defer proposal.Close()
	if s.Leader == nil || s.Leader.Name == proposal.Input().Candidate || !s.candidateExists(proposal.Input().Candidate) {
		proposal.Output(&electionprotocolv1.PromoteOutput{
			Term: s.term(),
		})
		return
	}

	if s.Candidates[0].Name == proposal.Input().Candidate {
		s.Term++
		s.Leader = &s.Candidates[0]
		s.Candidates = s.Candidates[1:]
	} else {
		var index int
		for i, candidate := range s.Candidates {
			if candidate.Name == proposal.Input().Candidate {
				index = i
				break
			}
		}

		candidates := make([]electionprotocolv1.LeaderElectionCandidate, 0, len(s.Candidates))
		for i, candidate := range s.Candidates {
			if i < index-1 {
				candidates[i] = candidate
			} else if i == index-1 {
				candidates[i] = s.Candidates[index]
			} else if i == index {
				candidates[i] = s.Candidates[i-1]
			} else {
				candidates[i] = candidate
			}
		}
		s.Candidates = candidates
	}

	term := s.term()
	s.notify(term)
	proposal.Output(&electionprotocolv1.PromoteOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) Demote(proposal statemachine.Proposal[*electionprotocolv1.DemoteInput, *electionprotocolv1.DemoteOutput]) {
	defer proposal.Close()
	if s.Leader == nil || s.Leader.Name == proposal.Input().Candidate || !s.candidateExists(proposal.Input().Candidate) {
		proposal.Output(&electionprotocolv1.DemoteOutput{
			Term: s.term(),
		})
		return
	}

	if s.Leader != nil && s.Leader.Name == proposal.Input().Candidate {
		if len(s.Candidates) == 0 {
			proposal.Output(&electionprotocolv1.DemoteOutput{
				Term: s.term(),
			})
			return
		}
		leader := s.Leader
		s.Term++
		s.Leader = &s.Candidates[0]
		s.Candidates = append([]electionprotocolv1.LeaderElectionCandidate{*leader}, s.Candidates[1:]...)
	} else if s.Candidates[len(s.Candidates)-1].Name != proposal.Input().Candidate {
		var index int
		for i, candidate := range s.Candidates {
			if candidate.Name == proposal.Input().Candidate {
				index = i
				break
			}
		}

		candidates := make([]electionprotocolv1.LeaderElectionCandidate, 0, len(s.Candidates))
		for i, candidate := range s.Candidates {
			if i < index+1 {
				candidates[i] = candidate
			} else if i == index+1 {
				candidates[i] = s.Candidates[index]
			} else if i == index {
				candidates[i] = s.Candidates[i+1]
			} else {
				candidates[i] = candidate
			}
		}
		s.Candidates = candidates
	}

	term := s.term()
	s.notify(term)
	proposal.Output(&electionprotocolv1.DemoteOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) Evict(proposal statemachine.Proposal[*electionprotocolv1.EvictInput, *electionprotocolv1.EvictOutput]) {
	defer proposal.Close()
	if s.Leader == nil || (s.Leader.Name != proposal.Input().Candidate && !s.candidateExists(proposal.Input().Candidate)) {
		proposal.Output(&electionprotocolv1.EvictOutput{
			Term: s.term(),
		})
		return
	}

	if s.Leader.Name == proposal.Input().Candidate {
		s.Leader = nil
		if len(s.Candidates) > 0 {
			candidate := s.Candidates[0]
			s.Candidates = s.Candidates[1:]
			s.Leader = &candidate
			s.Term++
		}
	} else {
		candidates := make([]electionprotocolv1.LeaderElectionCandidate, 0, len(s.Candidates))
		for _, candidate := range s.Candidates {
			if candidate.Name != proposal.Input().Candidate {
				candidates = append(candidates, candidate)
			}
		}
		s.Candidates = candidates
	}

	term := s.term()
	s.notify(term)
	proposal.Output(&electionprotocolv1.EvictOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) GetTerm(query statemachine.Query[*electionprotocolv1.GetTermInput, *electionprotocolv1.GetTermOutput]) {
	defer query.Close()
	query.Output(&electionprotocolv1.GetTermOutput{
		Term: s.term(),
	})
}

func (s *leaderElectionStateMachine) Watch(query statemachine.Query[*electionprotocolv1.WatchInput, *electionprotocolv1.WatchOutput]) {
	s.mu.Lock()
	s.watchers[query.ID()] = query
	s.mu.Unlock()
	if s.Term > 0 {
		query.Output(&electionprotocolv1.WatchOutput{
			Term: s.term(),
		})
	}
}

func (s *leaderElectionStateMachine) notify(term electionprotocolv1.Term) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&electionprotocolv1.WatchOutput{
			Term: term,
		})
	}
}
