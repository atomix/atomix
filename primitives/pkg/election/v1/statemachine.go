// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"sync"
)

const Service = "atomix.runtime.election.v1.LeaderElection"

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*LeaderElectionInput, *LeaderElectionOutput](registry)(PrimitiveType)
}

var PrimitiveType = statemachine.NewPrimitiveType[*LeaderElectionInput, *LeaderElectionOutput](Service, lockCodec,
	func(context statemachine.PrimitiveContext[*LeaderElectionInput, *LeaderElectionOutput]) statemachine.Executor[*LeaderElectionInput, *LeaderElectionOutput] {
		return newExecutor(NewLeaderElectionStateMachine(context))
	})

type LeaderElectionContext interface {
	statemachine.PrimitiveContext[*LeaderElectionInput, *LeaderElectionOutput]
}

type LeaderElectionStateMachine interface {
	statemachine.Context[*LeaderElectionInput, *LeaderElectionOutput]
	statemachine.Recoverable
	Enter(statemachine.Proposal[*EnterInput, *EnterOutput])
	Withdraw(statemachine.Proposal[*WithdrawInput, *WithdrawOutput])
	Anoint(statemachine.Proposal[*AnointInput, *AnointOutput])
	Promote(statemachine.Proposal[*PromoteInput, *PromoteOutput])
	Demote(statemachine.Proposal[*DemoteInput, *DemoteOutput])
	Evict(statemachine.Proposal[*EvictInput, *EvictOutput])
	GetTerm(statemachine.Query[*GetTermInput, *GetTermOutput])
	Watch(statemachine.Query[*WatchInput, *WatchOutput])
}

func NewLeaderElectionStateMachine(context statemachine.PrimitiveContext[*LeaderElectionInput, *LeaderElectionOutput]) LeaderElectionStateMachine {
	return &leaderElectionStateMachine{
		LeaderElectionContext: context,
		watchers:              make(map[statemachine.QueryID]statemachine.Query[*WatchInput, *WatchOutput]),
	}
}

type leaderElectionStateMachine struct {
	LeaderElectionContext
	LeaderElectionSnapshot
	cancel   statemachine.CancelFunc
	watchers map[statemachine.QueryID]statemachine.Query[*WatchInput, *WatchOutput]
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

func (s *leaderElectionStateMachine) term() Term {
	term := Term{
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

func (s *leaderElectionStateMachine) Enter(proposal statemachine.Proposal[*EnterInput, *EnterOutput]) {
	defer proposal.Close()
	if s.Leader == nil {
		s.Term++
		s.Leader = &LeaderElectionCandidate{
			Name:      proposal.Input().Candidate,
			SessionID: protocol.SessionID(proposal.Session().ID()),
		}
		if err := s.watchSession(proposal.Session().ID()); err != nil {
			panic(err)
		}
		term := s.term()
		s.notify(term)
		proposal.Output(&EnterOutput{
			Term: term,
		})
	} else if !s.candidateExists(proposal.Input().Candidate) {
		s.Candidates = append(s.Candidates, LeaderElectionCandidate{
			Name:      proposal.Input().Candidate,
			SessionID: protocol.SessionID(proposal.Session().ID()),
		})
		term := s.term()
		s.notify(term)
		proposal.Output(&EnterOutput{
			Term: term,
		})
	} else {
		proposal.Output(&EnterOutput{
			Term: s.term(),
		})
	}
}

func (s *leaderElectionStateMachine) Withdraw(proposal statemachine.Proposal[*WithdrawInput, *WithdrawOutput]) {
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
		proposal.Output(&WithdrawOutput{
			Term: term,
		})
	} else {
		proposal.Output(&WithdrawOutput{
			Term: s.term(),
		})
	}
}

func (s *leaderElectionStateMachine) Anoint(proposal statemachine.Proposal[*AnointInput, *AnointOutput]) {
	defer proposal.Close()
	if s.Leader == nil || s.Leader.Name == proposal.Input().Candidate || !s.candidateExists(proposal.Input().Candidate) {
		proposal.Output(&AnointOutput{
			Term: s.term(),
		})
		return
	}

	candidates := make([]LeaderElectionCandidate, 0, len(s.Candidates))
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
	proposal.Output(&AnointOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) Promote(proposal statemachine.Proposal[*PromoteInput, *PromoteOutput]) {
	defer proposal.Close()
	if s.Leader == nil || s.Leader.Name == proposal.Input().Candidate || !s.candidateExists(proposal.Input().Candidate) {
		proposal.Output(&PromoteOutput{
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

		candidates := make([]LeaderElectionCandidate, 0, len(s.Candidates))
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
	proposal.Output(&PromoteOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) Demote(proposal statemachine.Proposal[*DemoteInput, *DemoteOutput]) {
	defer proposal.Close()
	if s.Leader == nil || s.Leader.Name == proposal.Input().Candidate || !s.candidateExists(proposal.Input().Candidate) {
		proposal.Output(&DemoteOutput{
			Term: s.term(),
		})
		return
	}

	if s.Leader != nil && s.Leader.Name == proposal.Input().Candidate {
		if len(s.Candidates) == 0 {
			proposal.Output(&DemoteOutput{
				Term: s.term(),
			})
			return
		}
		leader := s.Leader
		s.Term++
		s.Leader = &s.Candidates[0]
		s.Candidates = append([]LeaderElectionCandidate{*leader}, s.Candidates[1:]...)
	} else if s.Candidates[len(s.Candidates)-1].Name != proposal.Input().Candidate {
		var index int
		for i, candidate := range s.Candidates {
			if candidate.Name == proposal.Input().Candidate {
				index = i
				break
			}
		}

		candidates := make([]LeaderElectionCandidate, 0, len(s.Candidates))
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
	proposal.Output(&DemoteOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) Evict(proposal statemachine.Proposal[*EvictInput, *EvictOutput]) {
	defer proposal.Close()
	if s.Leader == nil || (s.Leader.Name != proposal.Input().Candidate && !s.candidateExists(proposal.Input().Candidate)) {
		proposal.Output(&EvictOutput{
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
		candidates := make([]LeaderElectionCandidate, 0, len(s.Candidates))
		for _, candidate := range s.Candidates {
			if candidate.Name != proposal.Input().Candidate {
				candidates = append(candidates, candidate)
			}
		}
		s.Candidates = candidates
	}

	term := s.term()
	s.notify(term)
	proposal.Output(&EvictOutput{
		Term: term,
	})
}

func (s *leaderElectionStateMachine) GetTerm(query statemachine.Query[*GetTermInput, *GetTermOutput]) {
	defer query.Close()
	query.Output(&GetTermOutput{
		Term: s.term(),
	})
}

func (s *leaderElectionStateMachine) Watch(query statemachine.Query[*WatchInput, *WatchOutput]) {
	s.mu.Lock()
	s.watchers[query.ID()] = query
	s.mu.Unlock()
	if s.Term > 0 {
		query.Output(&WatchOutput{
			Term: s.term(),
		})
	}
}

func (s *leaderElectionStateMachine) notify(term Term) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, watcher := range s.watchers {
		watcher.Output(&WatchOutput{
			Term: term,
		})
	}
}
