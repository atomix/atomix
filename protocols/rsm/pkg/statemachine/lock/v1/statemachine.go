// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/errors"
	lockprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/lock/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/statemachine"
)

const (
	Name       = "Lock"
	APIVersion = "v1"
)

var PrimitiveType = protocol.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterStateMachine(registry *statemachine.PrimitiveTypeRegistry) {
	statemachine.RegisterPrimitiveType[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput](registry)(PrimitiveType,
		func(context statemachine.PrimitiveContext[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]) statemachine.PrimitiveStateMachine[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput] {
			return newExecutor(NewLockStateMachine(context))
		}, lockCodec)
}

type LockContext interface {
	statemachine.PrimitiveContext[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]
	Requests() statemachine.Proposals[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]
}

func newContext(context statemachine.PrimitiveContext[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]) LockContext {
	return &lockContext{
		PrimitiveContext: context,
		requests: statemachine.NewProposals[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput, *lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput](context).
			Decoder(func(input *lockprotocolv1.LockInput) (*lockprotocolv1.AcquireInput, bool) {
				if events, ok := input.Input.(*lockprotocolv1.LockInput_Acquire); ok {
					return events.Acquire, true
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
			Build(),
	}
}

type lockContext struct {
	statemachine.PrimitiveContext[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]
	requests statemachine.Proposals[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]
}

func (c *lockContext) Requests() statemachine.Proposals[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput] {
	return c.requests
}

type LockStateMachine interface {
	statemachine.Context[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]
	statemachine.Recoverable
	Acquire(statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput])
	Release(statemachine.Proposal[*lockprotocolv1.ReleaseInput, *lockprotocolv1.ReleaseOutput])
	Get(statemachine.Query[*lockprotocolv1.GetInput, *lockprotocolv1.GetOutput])
}

func NewLockStateMachine(context statemachine.PrimitiveContext[*lockprotocolv1.LockInput, *lockprotocolv1.LockOutput]) LockStateMachine {
	return &lockStateMachine{
		LockContext: newContext(context),
		proposals:   make(map[statemachine.ProposalID]statemachine.CancelFunc),
		timers:      make(map[statemachine.ProposalID]statemachine.CancelFunc),
	}
}

type lock struct {
	proposalID statemachine.ProposalID
	sessionID  statemachine.SessionID
	watcher    statemachine.CancelFunc
}

type lockStateMachine struct {
	LockContext
	lock      *lock
	queue     []statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]
	proposals map[statemachine.ProposalID]statemachine.CancelFunc
	timers    map[statemachine.ProposalID]statemachine.CancelFunc
}

func (s *lockStateMachine) Snapshot(writer *statemachine.SnapshotWriter) error {
	if s.lock != nil {
		if err := writer.WriteBool(true); err != nil {
			return err
		}
		if err := writer.WriteVarUint64(uint64(s.lock.proposalID)); err != nil {
			return err
		}
		if err := writer.WriteVarUint64(uint64(s.lock.sessionID)); err != nil {
			return err
		}
		if err := writer.WriteVarInt(len(s.queue)); err != nil {
			return err
		}
		for _, waiter := range s.queue {
			if err := writer.WriteVarUint64(uint64(waiter.ID())); err != nil {
				return err
			}
		}
	} else {
		if err := writer.WriteBool(false); err != nil {
			return err
		}
	}
	return nil
}

func (s *lockStateMachine) Recover(reader *statemachine.SnapshotReader) error {
	locked, err := reader.ReadBool()
	if err != nil {
		return err
	}

	if locked {
		proposalID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}
		sessionID, err := reader.ReadVarUint64()
		if err != nil {
			return err
		}

		session, ok := s.Sessions().Get(statemachine.SessionID(sessionID))
		if !ok {
			return errors.NewFault("session not found")
		}

		s.lock = &lock{
			proposalID: statemachine.ProposalID(proposalID),
			sessionID:  statemachine.SessionID(sessionID),
			watcher: session.Watch(func(state statemachine.State) {
				if state == statemachine.Closed {
					s.nextRequest()
				}
			}),
		}

		n, err := reader.ReadVarInt()
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			proposalID, err := reader.ReadVarUint64()
			if err != nil {
				return err
			}
			proposal, ok := s.Requests().Get(statemachine.ProposalID(proposalID))
			if !ok {
				return errors.NewFault("proposal not found")
			}
			s.enqueueRequest(proposal)
		}
	}
	return nil
}

func (s *lockStateMachine) enqueueRequest(proposal statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]) {
	s.queue = append(s.queue, proposal)
	s.watchRequest(proposal)
}

func (s *lockStateMachine) dequeueRequest(proposal statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]) {
	s.unwatchRequest(proposal.ID())
	queue := make([]statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput], 0, len(s.queue))
	for _, waiter := range s.queue {
		if waiter.ID() != proposal.ID() {
			queue = append(queue, waiter)
		}
	}
	s.queue = queue
}

func (s *lockStateMachine) nextRequest() {
	s.lock = nil
	if len(s.queue) == 0 {
		return
	}
	proposal := s.queue[0]
	s.lock = &lock{
		proposalID: proposal.ID(),
		sessionID:  proposal.Session().ID(),
		watcher: proposal.Session().Watch(func(state statemachine.State) {
			if state == statemachine.Closed {
				s.nextRequest()
			}
		}),
	}
	s.queue = s.queue[1:]
	s.unwatchRequest(proposal.ID())
	proposal.Output(&lockprotocolv1.AcquireOutput{
		Index: protocol.Index(proposal.ID()),
	})
	proposal.Close()
}

func (s *lockStateMachine) watchRequest(proposal statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]) {
	s.proposals[proposal.ID()] = proposal.Watch(func(state statemachine.ProposalState) {
		if state != statemachine.Running {
			s.dequeueRequest(proposal)
		}
	})

	if proposal.Input().Timeout != nil {
		s.timers[proposal.ID()] = s.Scheduler().Delay(*proposal.Input().Timeout, func() {
			s.dequeueRequest(proposal)
			proposal.Error(errors.NewConflict("lock already held"))
			proposal.Close()
		})
	}
}

func (s *lockStateMachine) unwatchRequest(proposalID statemachine.ProposalID) {
	if cancel, ok := s.proposals[proposalID]; ok {
		cancel()
		delete(s.proposals, proposalID)
	}
	if cancel, ok := s.timers[proposalID]; ok {
		cancel()
		delete(s.timers, proposalID)
	}
}

func (s *lockStateMachine) Acquire(proposal statemachine.Proposal[*lockprotocolv1.AcquireInput, *lockprotocolv1.AcquireOutput]) {
	if s.lock == nil {
		s.lock = &lock{
			proposalID: proposal.ID(),
			sessionID:  proposal.Session().ID(),
			watcher: proposal.Session().Watch(func(state statemachine.State) {
				if state == statemachine.Closed {
					s.nextRequest()
				}
			}),
		}
		proposal.Output(&lockprotocolv1.AcquireOutput{
			Index: protocol.Index(proposal.ID()),
		})
		proposal.Close()
	} else {
		s.enqueueRequest(proposal)
	}
}

func (s *lockStateMachine) Release(proposal statemachine.Proposal[*lockprotocolv1.ReleaseInput, *lockprotocolv1.ReleaseOutput]) {
	defer proposal.Close()
	if s.lock == nil || s.lock.sessionID != proposal.Session().ID() {
		proposal.Error(errors.NewConflict("lock not held by client"))
	} else {
		s.nextRequest()
		proposal.Output(&lockprotocolv1.ReleaseOutput{})
	}
}

func (s *lockStateMachine) Get(query statemachine.Query[*lockprotocolv1.GetInput, *lockprotocolv1.GetOutput]) {
	defer query.Close()
	if s.lock != nil {
		query.Output(&lockprotocolv1.GetOutput{
			Index: protocol.Index(s.lock.proposalID),
		})
	} else {
		query.Error(errors.NewNotFound("local not held"))
	}
}
