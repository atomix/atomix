// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/atomix/api/errors"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/gogo/protobuf/types"
	"sync"
	"sync/atomic"
	"time"
)

var log = logging.GetLogger()

type Context[I, O any] interface {
	// Index returns the current service index
	Index() protocol.Index
	// Time returns the current service time
	Time() time.Time
	// Scheduler returns the service scheduler
	Scheduler() Scheduler
	// Sessions returns the open sessions
	Sessions() Sessions
	// Proposals returns the pending proposals
	Proposals() Proposals[I, O]
}

type StateMachine interface {
	Recoverable
	Propose(input *protocol.ProposalInput, stream streams.WriteStream[*protocol.ProposalOutput])
	Query(input *protocol.QueryInput, stream streams.WriteStream[*protocol.QueryOutput])
}

func NewStateMachine(registry *PrimitiveTypeRegistry) StateMachine {
	return newStateMachine(func(ctx Context[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) PrimitiveManager {
		return newPrimitiveManager(ctx, registry)
	})
}

func newStateMachine(factory func(Context[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) PrimitiveManager) StateMachine {
	return (&sessionManager{factory: factory}).init()
}

type sessionManager struct {
	factory        func(Context[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) PrimitiveManager
	index          protocol.Index
	sequenceNum    atomic.Uint64
	scheduler      *stateMachineScheduler
	sessions       *managedSessions
	proposals      *sessionProposals
	primitives     PrimitiveManager
	pendingQueries sync.Map
}

func (s *sessionManager) init() StateMachine {
	s.reset()
	return s
}

func (s *sessionManager) reset() {
	s.scheduler = newScheduler()
	s.sessions = newManagedSessions()
	s.proposals = newSessionProposals()
	s.primitives = s.factory(s)
}

func (s *sessionManager) Log() logging.Logger {
	return log
}

func (s *sessionManager) Index() protocol.Index {
	return s.index
}

func (s *sessionManager) Time() time.Time {
	return s.scheduler.Time()
}

func (s *sessionManager) Scheduler() Scheduler {
	return s.scheduler
}

func (s *sessionManager) Sessions() Sessions {
	return s.sessions
}

func (s *sessionManager) Proposals() Proposals[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput] {
	return s.proposals
}

func (s *sessionManager) Snapshot(writer *SnapshotWriter) error {
	if err := writer.WriteVarUint64(uint64(s.index)); err != nil {
		return err
	}

	timestamp, err := types.TimestampProto(s.Time())
	if err != nil {
		return err
	}
	if err := writer.WriteMessage(timestamp); err != nil {
		return err
	}

	sessions := s.sessions.list()
	if err := writer.WriteVarInt(len(sessions)); err != nil {
		return err
	}
	for _, session := range sessions {
		if err := session.Snapshot(writer); err != nil {
			return err
		}
	}
	return s.primitives.Snapshot(writer)
}

func (s *sessionManager) Recover(reader *SnapshotReader) error {
	s.reset()

	index, err := reader.ReadVarUint64()
	if err != nil {
		return err
	}
	s.index = protocol.Index(index)

	timestamp := &types.Timestamp{}
	if err := reader.ReadMessage(timestamp); err != nil {
		return err
	}
	t, err := types.TimestampFromProto(timestamp)
	if err != nil {
		return err
	}
	s.scheduler.time.Store(t.Local())

	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		session := newManagedSession(s)
		if err := session.Recover(reader); err != nil {
			return err
		}
	}
	return s.primitives.Recover(reader)
}

func (s *sessionManager) Propose(input *protocol.ProposalInput, stream streams.WriteStream[*protocol.ProposalOutput]) {
	// Run scheduled tasks for the updated timestamp
	s.scheduler.tick(input.Timestamp)

	switch p := input.Input.(type) {
	case *protocol.ProposalInput_Proposal:
		s.propose(p.Proposal, streams.NewEncodingStream[*protocol.SessionProposalOutput, *protocol.ProposalOutput](stream, func(output *protocol.SessionProposalOutput, err error) (*protocol.ProposalOutput, error) {
			if err != nil {
				return nil, err
			}
			return &protocol.ProposalOutput{
				Index: s.index,
				Output: &protocol.ProposalOutput_Proposal{
					Proposal: output,
				},
			}, nil
		}))
	case *protocol.ProposalInput_OpenSession:
		s.openSession(p.OpenSession, streams.NewEncodingStream[*protocol.OpenSessionOutput, *protocol.ProposalOutput](stream, func(output *protocol.OpenSessionOutput, err error) (*protocol.ProposalOutput, error) {
			if err != nil {
				return nil, err
			}
			return &protocol.ProposalOutput{
				Index: s.index,
				Output: &protocol.ProposalOutput_OpenSession{
					OpenSession: output,
				},
			}, nil
		}))
	case *protocol.ProposalInput_KeepAlive:
		s.keepAlive(p.KeepAlive, streams.NewEncodingStream[*protocol.KeepAliveOutput, *protocol.ProposalOutput](stream, func(output *protocol.KeepAliveOutput, err error) (*protocol.ProposalOutput, error) {
			if err != nil {
				return nil, err
			}
			return &protocol.ProposalOutput{
				Index: s.index,
				Output: &protocol.ProposalOutput_KeepAlive{
					KeepAlive: output,
				},
			}, nil
		}))
	case *protocol.ProposalInput_CloseSession:
		s.closeSession(p.CloseSession, streams.NewEncodingStream[*protocol.CloseSessionOutput, *protocol.ProposalOutput](stream, func(output *protocol.CloseSessionOutput, err error) (*protocol.ProposalOutput, error) {
			if err != nil {
				return nil, err
			}
			return &protocol.ProposalOutput{
				Index: s.index,
				Output: &protocol.ProposalOutput_CloseSession{
					CloseSession: output,
				},
			}, nil
		}))
	}

	if indexQueries, ok := s.pendingQueries.LoadAndDelete(s.index); ok {
		indexQueries.(*sync.Map).Range(func(key, value any) bool {
			sequenceNum := key.(protocol.SequenceNum)
			query := value.(pendingQuery)
			switch q := query.input.Input.(type) {
			case *protocol.QueryInput_Query:
				s.query(sequenceNum, q.Query, streams.NewEncodingStream[*protocol.SessionQueryOutput, *protocol.QueryOutput](query.stream, func(output *protocol.SessionQueryOutput, err error) (*protocol.QueryOutput, error) {
					if err != nil {
						return nil, err
					}
					return &protocol.QueryOutput{
						Index: s.index,
						Output: &protocol.QueryOutput_Query{
							Query: output,
						},
					}, nil
				}))
			}
			return true
		})
	}
}

type pendingQuery struct {
	input  *protocol.QueryInput
	stream streams.WriteStream[*protocol.QueryOutput]
}

func (s *sessionManager) Query(input *protocol.QueryInput, stream streams.WriteStream[*protocol.QueryOutput]) {
	sequenceNum := protocol.SequenceNum(s.sequenceNum.Add(1))
	if s.index < input.MaxReceivedIndex {
		indexQueries, _ := s.pendingQueries.LoadOrStore(s.index, &sync.Map{})
		indexQueries.(*sync.Map).Store(sequenceNum, pendingQuery{
			input:  input,
			stream: stream,
		})
	} else {
		switch q := input.Input.(type) {
		case *protocol.QueryInput_Query:
			s.query(sequenceNum, q.Query, streams.NewEncodingStream[*protocol.SessionQueryOutput, *protocol.QueryOutput](stream, func(output *protocol.SessionQueryOutput, err error) (*protocol.QueryOutput, error) {
				if err != nil {
					return nil, err
				}
				return &protocol.QueryOutput{
					Index: s.index,
					Output: &protocol.QueryOutput_Query{
						Query: output,
					},
				}, nil
			}))
		}
	}
}

func (s *sessionManager) openSession(input *protocol.OpenSessionInput, stream streams.WriteStream[*protocol.OpenSessionOutput]) {
	session := newManagedSession(s)
	session.open(s.index, input, stream)
}

func (s *sessionManager) keepAlive(input *protocol.KeepAliveInput, stream streams.WriteStream[*protocol.KeepAliveOutput]) {
	sessionID := SessionID(input.SessionID)
	session, ok := s.sessions.get(sessionID)
	if !ok {
		stream.Error(errors.NewFault("session not found"))
		stream.Close()
	} else {
		session.keepAlive(s.index, input, stream)
	}
}

func (s *sessionManager) closeSession(input *protocol.CloseSessionInput, stream streams.WriteStream[*protocol.CloseSessionOutput]) {
	sessionID := SessionID(input.SessionID)
	session, ok := s.sessions.get(sessionID)
	if !ok {
		stream.Error(errors.NewFault("session not found"))
		stream.Close()
	} else {
		session.close(s.index, input, stream)
	}
}

func (s *sessionManager) propose(input *protocol.SessionProposalInput, stream streams.WriteStream[*protocol.SessionProposalOutput]) {
	sessionID := SessionID(input.SessionID)
	session, ok := s.sessions.get(sessionID)
	if !ok {
		stream.Error(errors.NewFault("session not found"))
		stream.Close()
	} else {
		session.propose(s.index, input, stream)
	}
}

func (s *sessionManager) query(sequenceNum protocol.SequenceNum, input *protocol.SessionQueryInput, stream streams.WriteStream[*protocol.SessionQueryOutput]) {
	sessionID := SessionID(input.SessionID)
	session, ok := s.sessions.get(sessionID)
	if !ok {
		stream.Error(errors.NewFault("session not found"))
		stream.Close()
	} else {
		session.query(sequenceNum, input, stream)
	}
}
