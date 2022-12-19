// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/google/uuid"
	"sync"
	"sync/atomic"
	"time"
)

type CallState int

const (
	Pending CallState = iota
	Running
	Complete
	Canceled
)

type CallID interface {
	ProposalID | QueryID
}

type ProposalID uint64

type QueryID uint64

// Call is a proposal or query call context
type Call[T CallID, I, O any] interface {
	// ID returns the execution identifier
	ID() T
	// Log returns the operation log
	Log() logging.Logger
	// Time returns the state machine time at the time of the call
	Time() time.Time
	// Session returns the call session
	Session() Session
	// State returns the call state
	State() CallState
	// Watch watches the call state for changes
	Watch(watcher func(CallState)) CancelFunc
	// Input returns the input
	Input() I
	// Output returns the output
	Output(O)
	// Error returns a failure error
	Error(error)
	// Cancel cancels the call
	Cancel()
	// Close closes the execution
	Close()
}

type ProposalState = CallState

type Proposal[I, O any] interface {
	Call[ProposalID, I, O]
}

type QueryState = CallState

type Query[I, O any] interface {
	Call[QueryID, I, O]
}

// Proposals provides access to pending proposals
type Proposals[I, O any] interface {
	// Get gets a proposal by ID
	Get(id ProposalID) (Proposal[I, O], bool)
	// List lists all open proposals
	List() []Proposal[I, O]
}

type SessionID uint64

type State int

const (
	Open State = iota
	Closed
)

type SessionContext Context[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]

// Session is a service session
type Session interface {
	// Log returns the session log
	Log() logging.Logger
	// ID returns the session identifier
	ID() SessionID
	// State returns the current session state
	State() State
	// Watch watches the session state for changes
	Watch(watcher func(State)) CancelFunc
}

// Sessions provides access to open sessions
type Sessions interface {
	// Get gets a session by ID
	Get(SessionID) (Session, bool)
	// List lists all open sessions
	List() []Session
}

type SessionManager interface {
	Recoverable
	OpenSession(input *protocol.OpenSessionInput, stream streams.WriteStream[*protocol.OpenSessionOutput])
	KeepAlive(input *protocol.KeepAliveInput, stream streams.WriteStream[*protocol.KeepAliveOutput])
	CloseSession(input *protocol.CloseSessionInput, stream streams.WriteStream[*protocol.CloseSessionOutput])
	Propose(input *protocol.SessionProposalInput, stream streams.WriteStream[*protocol.SessionProposalOutput])
	Query(input *protocol.SessionQueryInput, stream streams.WriteStream[*protocol.SessionQueryOutput])
}

func newManagedSession(manager *sessionManager) *managedSession {
	return &managedSession{
		manager:   manager,
		proposals: make(map[protocol.SequenceNum]*sessionProposal),
		queries:   make(map[protocol.SequenceNum]*sessionQuery),
		watchers:  make(map[uuid.UUID]func(State)),
	}
}

type managedSession struct {
	manager          *sessionManager
	proposals        map[protocol.SequenceNum]*sessionProposal
	queries          map[protocol.SequenceNum]*sessionQuery
	queriesMu        sync.Mutex
	log              logging.Logger
	id               SessionID
	state            State
	watchers         map[uuid.UUID]func(State)
	timeout          time.Duration
	lastUpdated      time.Time
	expireCancelFunc CancelFunc
}

func (s *managedSession) Log() logging.Logger {
	return s.log
}

func (s *managedSession) ID() SessionID {
	return s.id
}

func (s *managedSession) State() State {
	return s.state
}

func (s *managedSession) Watch(f func(State)) CancelFunc {
	id := uuid.New()
	s.watchers[id] = f
	return func() {
		delete(s.watchers, id)
	}
}

func (s *managedSession) propose(input *protocol.SessionProposalInput, stream streams.WriteStream[*protocol.SessionProposalOutput]) {
	if proposal, ok := s.proposals[input.SequenceNum]; ok {
		proposal.replay(stream)
	} else {
		proposal := newSessionProposal(s)
		s.proposals[input.SequenceNum] = proposal
		proposal.execute(input, stream)
	}
}

func (s *managedSession) query(input *protocol.SessionQueryInput, stream streams.WriteStream[*protocol.SessionQueryOutput]) {
	query := newSessionQuery(s)
	query.execute(input, stream)
	if query.state == Running {
		s.queriesMu.Lock()
		s.queries[query.Input().SequenceNum] = query
		s.queriesMu.Unlock()
	}
}

func (s *managedSession) Snapshot(writer *SnapshotWriter) error {
	s.Log().Debug("Persisting session to snapshot")

	var state protocol.SessionSnapshot_State
	switch s.state {
	case Open:
		state = protocol.SessionSnapshot_OPEN
	case Closed:
		state = protocol.SessionSnapshot_CLOSED
	}

	snapshot := &protocol.SessionSnapshot{
		SessionID:   protocol.SessionID(s.id),
		State:       state,
		Timeout:     s.timeout,
		LastUpdated: s.lastUpdated,
	}
	if err := writer.WriteMessage(snapshot); err != nil {
		return err
	}

	if err := writer.WriteVarInt(len(s.proposals)); err != nil {
		return err
	}
	for _, proposal := range s.proposals {
		if err := proposal.snapshot(writer); err != nil {
			return err
		}
	}
	return nil
}

func (s *managedSession) Recover(reader *SnapshotReader) error {
	snapshot := &protocol.SessionSnapshot{}
	if err := reader.ReadMessage(snapshot); err != nil {
		return err
	}

	s.id = SessionID(snapshot.SessionID)
	s.timeout = snapshot.Timeout
	s.lastUpdated = snapshot.LastUpdated

	s.log = log.WithFields(logging.Uint64("Session", uint64(s.id)))
	s.Log().Debug("Recovering session from snapshot")

	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		proposal := newSessionProposal(s)
		if err := proposal.recover(reader); err != nil {
			return err
		}
		s.proposals[proposal.input.SequenceNum] = proposal
	}

	switch snapshot.State {
	case protocol.SessionSnapshot_OPEN:
		s.state = Open
	case protocol.SessionSnapshot_CLOSED:
		s.state = Closed
	}
	s.manager.sessions.add(s)
	s.scheduleExpireTimer()
	return nil
}

func (s *managedSession) expire() {
	s.Log().Warnf("Session expired after %s", s.manager.Time().Sub(s.lastUpdated))
	s.destroy()
}

func (s *managedSession) scheduleExpireTimer() {
	if s.expireCancelFunc != nil {
		s.expireCancelFunc()
	}
	expireTime := s.lastUpdated.Add(s.timeout)
	s.expireCancelFunc = s.manager.Scheduler().Schedule(expireTime, s.expire)
	s.Log().Debugw("Scheduled expire time", logging.Time("Expire", expireTime))
}

func (s *managedSession) open(input *protocol.OpenSessionInput, stream streams.WriteStream[*protocol.OpenSessionOutput]) {
	defer stream.Close()
	s.id = SessionID(s.manager.Index())
	s.state = Open
	s.lastUpdated = s.manager.Time()
	s.timeout = input.Timeout
	s.log = log.WithFields(logging.Uint64("Session", uint64(s.id)))
	s.manager.sessions.add(s)
	s.scheduleExpireTimer()
	s.Log().Infow("Opened session", logging.Duration("Timeout", s.timeout))
	stream.Value(&protocol.OpenSessionOutput{
		SessionID: protocol.SessionID(s.ID()),
	})
}

func (s *managedSession) keepAlive(input *protocol.KeepAliveInput, stream streams.WriteStream[*protocol.KeepAliveOutput]) {
	defer stream.Close()

	openInputs := &bloom.BloomFilter{}
	if err := json.Unmarshal(input.InputFilter, openInputs); err != nil {
		s.Log().Warn("Failed to decode request filter", err)
		stream.Error(errors.NewInvalid("invalid request filter", err))
		return
	}

	s.Log().Debug("Processing keep-alive")
	for _, proposal := range s.proposals {
		if input.LastInputSequenceNum < proposal.Input().SequenceNum {
			continue
		}
		sequenceNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(sequenceNumBytes, uint64(proposal.Input().SequenceNum))
		if !openInputs.Test(sequenceNumBytes) {
			proposal.Cancel()
			delete(s.proposals, proposal.Input().SequenceNum)
		} else {
			if outputSequenceNum, ok := input.LastOutputSequenceNums[proposal.Input().SequenceNum]; ok {
				proposal.ack(outputSequenceNum)
			}
		}
	}

	s.queriesMu.Lock()
	for sn, query := range s.queries {
		if input.LastInputSequenceNum < query.Input().SequenceNum {
			continue
		}
		sequenceNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(sequenceNumBytes, uint64(query.Input().SequenceNum))
		if !openInputs.Test(sequenceNumBytes) {
			query.Cancel()
			delete(s.queries, sn)
		}
	}
	s.queriesMu.Unlock()

	stream.Value(&protocol.KeepAliveOutput{})

	s.lastUpdated = s.manager.Time()
	s.scheduleExpireTimer()
}

func (s *managedSession) close(input *protocol.CloseSessionInput, stream streams.WriteStream[*protocol.CloseSessionOutput]) {
	defer stream.Close()
	s.destroy()
	stream.Value(&protocol.CloseSessionOutput{})
}

func (s *managedSession) destroy() {
	s.manager.sessions.remove(s.id)
	s.expireCancelFunc()
	s.state = Closed
	for _, proposal := range s.proposals {
		proposal.Cancel()
	}
	s.queriesMu.Lock()
	for _, query := range s.queries {
		query.Cancel()
	}
	s.queriesMu.Unlock()
	for _, watcher := range s.watchers {
		watcher(Closed)
	}
}

func newManagedSessions() *managedSessions {
	return &managedSessions{
		sessions: make(map[SessionID]*managedSession),
	}
}

type managedSessions struct {
	sessions map[SessionID]*managedSession
}

func (s *managedSessions) Get(id SessionID) (Session, bool) {
	session, ok := s.sessions[id]
	return session, ok
}

func (s *managedSessions) List() []Session {
	sessions := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func (s *managedSessions) add(session *managedSession) bool {
	if _, ok := s.sessions[session.ID()]; !ok {
		s.sessions[session.ID()] = session
		return true
	}
	return false
}

func (s *managedSessions) remove(id SessionID) bool {
	if _, ok := s.sessions[id]; ok {
		delete(s.sessions, id)
		return true
	}
	return false
}

func (s *managedSessions) get(id SessionID) (*managedSession, bool) {
	session, ok := s.sessions[id]
	return session, ok
}

func (s *managedSessions) list() []*managedSession {
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func newSessionProposals() *sessionProposals {
	return &sessionProposals{
		proposals: make(map[ProposalID]*sessionPrimitiveProposal),
	}
}

type sessionProposals struct {
	proposals map[ProposalID]*sessionPrimitiveProposal
}

func (p *sessionProposals) Get(id ProposalID) (Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput], bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *sessionProposals) List() []Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput] {
	proposals := make([]Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput], 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

func (p *sessionProposals) add(proposal *sessionPrimitiveProposal) {
	p.proposals[proposal.ID()] = proposal
}

func (p *sessionProposals) remove(id ProposalID) {
	delete(p.proposals, id)
}

func newSessionProposal(session *managedSession) *sessionProposal {
	return &sessionProposal{
		session: session,
		outputs: list.New(),
	}
}

type sessionProposal struct {
	session      *managedSession
	id           ProposalID
	input        *protocol.SessionProposalInput
	timestamp    time.Time
	state        ProposalState
	stream       streams.WriteStream[*protocol.SessionProposalOutput]
	watchers     map[uuid.UUID]func(ProposalState)
	outputs      *list.List
	outputSeqNum protocol.SequenceNum
	log          logging.Logger
}

func (p *sessionProposal) ID() ProposalID {
	return p.id
}

func (p *sessionProposal) Log() logging.Logger {
	return p.log
}

func (p *sessionProposal) Session() Session {
	return p.session
}

func (p *sessionProposal) Time() time.Time {
	return p.timestamp
}

func (p *sessionProposal) State() ProposalState {
	return p.state
}

func (p *sessionProposal) Watch(watcher func(ProposalState)) CancelFunc {
	if p.watchers == nil {
		p.watchers = make(map[uuid.UUID]func(ProposalState))
	}
	id := uuid.New()
	p.watchers[id] = watcher
	return func() {
		delete(p.watchers, id)
	}
}

func (p *sessionProposal) execute(input *protocol.SessionProposalInput, stream streams.WriteStream[*protocol.SessionProposalOutput]) {
	p.id = ProposalID(p.session.manager.Index())
	p.input = input
	p.timestamp = p.session.manager.Time()
	p.state = Running
	p.log = p.session.Log().WithFields(logging.Uint64("ProposalID", uint64(p.id)))
	p.stream = stream

	switch input.Input.(type) {
	case *protocol.SessionProposalInput_Proposal:
		proposal := newSessionPrimitiveProposal(p)
		p.session.manager.proposals.add(proposal)
		p.session.manager.primitives.Propose(proposal)
	case *protocol.SessionProposalInput_CreatePrimitive:
		p.session.manager.primitives.CreatePrimitive(newCreatePrimitiveProposal(p))
	case *protocol.SessionProposalInput_ClosePrimitive:
		p.session.manager.primitives.ClosePrimitive(newClosePrimitiveProposal(p))
	}
}

func (p *sessionProposal) replay(stream streams.WriteStream[*protocol.SessionProposalOutput]) {
	p.stream = stream
	if p.outputs.Len() > 0 {
		p.Log().Debug("Replaying proposal outputs")
		elem := p.outputs.Front()
		for elem != nil {
			output := elem.Value.(*protocol.SessionProposalOutput)
			p.stream.Value(output)
			elem = elem.Next()
		}
	}
	if p.state == Complete {
		p.stream.Close()
	}
}

func (p *sessionProposal) snapshot(writer *SnapshotWriter) error {
	p.Log().Info("Persisting proposal to snapshot")
	pendingOutputs := make([]*protocol.SessionProposalOutput, 0, p.outputs.Len())
	elem := p.outputs.Front()
	for elem != nil {
		pendingOutputs = append(pendingOutputs, elem.Value.(*protocol.SessionProposalOutput))
		elem = elem.Next()
	}

	var phase protocol.SessionProposalSnapshot_Phase
	switch p.state {
	case Pending:
		phase = protocol.SessionProposalSnapshot_PENDING
	case Running:
		phase = protocol.SessionProposalSnapshot_RUNNING
	case Canceled:
		phase = protocol.SessionProposalSnapshot_CANCELED
	case Complete:
		phase = protocol.SessionProposalSnapshot_COMPLETE
	}

	snapshot := &protocol.SessionProposalSnapshot{
		Index:                 protocol.Index(p.ID()),
		Phase:                 phase,
		Input:                 p.input,
		PendingOutputs:        pendingOutputs,
		LastOutputSequenceNum: p.outputSeqNum,
		Timestamp:             p.timestamp,
	}
	return writer.WriteMessage(snapshot)
}

func (p *sessionProposal) recover(reader *SnapshotReader) error {
	snapshot := &protocol.SessionProposalSnapshot{}
	if err := reader.ReadMessage(snapshot); err != nil {
		return err
	}
	p.id = ProposalID(snapshot.Index)
	p.input = snapshot.Input
	p.timestamp = snapshot.Timestamp
	p.log = p.session.Log().WithFields(logging.Uint64("ProposalID", uint64(snapshot.Index)))
	p.Log().Info("Recovering command from snapshot")
	p.outputs = list.New()
	for _, output := range snapshot.PendingOutputs {
		r := output
		p.outputs.PushBack(r)
	}
	p.outputSeqNum = snapshot.LastOutputSequenceNum

	switch snapshot.Phase {
	case protocol.SessionProposalSnapshot_PENDING:
		p.state = Pending
	case protocol.SessionProposalSnapshot_RUNNING:
		p.state = Running
		switch p.input.Input.(type) {
		case *protocol.SessionProposalInput_Proposal:
			proposal := newSessionPrimitiveProposal(p)
			p.session.manager.proposals.add(proposal)
		}
	case protocol.SessionProposalSnapshot_COMPLETE:
		p.state = Complete
	case protocol.SessionProposalSnapshot_CANCELED:
		p.state = Canceled
	}
	return nil
}

func (p *sessionProposal) ack(outputSequenceNum protocol.SequenceNum) {
	p.Log().Debugw("Acked proposal outputs",
		logging.Uint64("SequenceNum", uint64(outputSequenceNum)))
	elem := p.outputs.Front()
	for elem != nil && elem.Value.(*protocol.SessionProposalOutput).SequenceNum <= outputSequenceNum {
		next := elem.Next()
		p.outputs.Remove(elem)
		elem = next
	}
}

func (p *sessionProposal) nextSequenceNum() protocol.SequenceNum {
	p.outputSeqNum++
	return p.outputSeqNum
}

func (p *sessionProposal) Input() *protocol.SessionProposalInput {
	return p.input
}

func (p *sessionProposal) Output(output *protocol.SessionProposalOutput) {
	if p.state != Running {
		return
	}
	p.Log().Debugw("Cached command output", logging.Uint64("SequenceNum", uint64(output.SequenceNum)))
	p.outputs.PushBack(output)
	if p.stream != nil {
		p.stream.Value(output)
	}
}

func (p *sessionProposal) Error(err error) {
	if p.state != Running {
		return
	}
	p.Output(&protocol.SessionProposalOutput{
		SequenceNum: p.nextSequenceNum(),
		Failure:     getFailure(err),
	})
	p.Close()
}

func (p *sessionProposal) Close() {
	p.close(Complete)
}

func (p *sessionProposal) Cancel() {
	p.close(Canceled)
}

func (p *sessionProposal) close(phase ProposalState) {
	if p.state != Running {
		return
	}
	if p.stream != nil {
		p.stream.Close()
	}
	p.state = phase
	p.session.manager.proposals.remove(p.id)
	if p.watchers != nil {
		for _, watcher := range p.watchers {
			watcher(phase)
		}
	}
}

func newSessionPrimitiveProposal(parent *sessionProposal) *sessionPrimitiveProposal {
	return &sessionPrimitiveProposal{
		sessionProposal: parent,
	}
}

type sessionPrimitiveProposal struct {
	*sessionProposal
}

func (p *sessionPrimitiveProposal) Input() *protocol.PrimitiveProposalInput {
	return p.sessionProposal.Input().GetProposal()
}

func (p *sessionPrimitiveProposal) Output(output *protocol.PrimitiveProposalOutput) {
	p.sessionProposal.Output(&protocol.SessionProposalOutput{
		SequenceNum: p.nextSequenceNum(),
		Output: &protocol.SessionProposalOutput_Proposal{
			Proposal: output,
		},
	})
}

var _ Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput] = (*sessionPrimitiveProposal)(nil)

func newCreatePrimitiveProposal(parent *sessionProposal) *createPrimitiveProposal {
	return &createPrimitiveProposal{
		sessionProposal: parent,
	}
}

type createPrimitiveProposal struct {
	*sessionProposal
}

func (p *createPrimitiveProposal) Input() *protocol.CreatePrimitiveInput {
	return p.sessionProposal.Input().GetCreatePrimitive()
}

func (p *createPrimitiveProposal) Output(output *protocol.CreatePrimitiveOutput) {
	p.sessionProposal.Output(&protocol.SessionProposalOutput{
		SequenceNum: p.nextSequenceNum(),
		Output: &protocol.SessionProposalOutput_CreatePrimitive{
			CreatePrimitive: output,
		},
	})
}

var _ Proposal[*protocol.CreatePrimitiveInput, *protocol.CreatePrimitiveOutput] = (*createPrimitiveProposal)(nil)

func newClosePrimitiveProposal(parent *sessionProposal) *closePrimitiveProposal {
	return &closePrimitiveProposal{
		sessionProposal: parent,
	}
}

type closePrimitiveProposal struct {
	*sessionProposal
}

func (p *closePrimitiveProposal) Input() *protocol.ClosePrimitiveInput {
	return p.sessionProposal.Input().GetClosePrimitive()
}

func (p *closePrimitiveProposal) Output(output *protocol.ClosePrimitiveOutput) {
	p.sessionProposal.Output(&protocol.SessionProposalOutput{
		SequenceNum: p.nextSequenceNum(),
		Output: &protocol.SessionProposalOutput_ClosePrimitive{
			ClosePrimitive: output,
		},
	})
}

var _ Proposal[*protocol.ClosePrimitiveInput, *protocol.ClosePrimitiveOutput] = (*closePrimitiveProposal)(nil)

func newSessionQuery(session *managedSession) *sessionQuery {
	return &sessionQuery{
		session: session,
	}
}

type sessionQuery struct {
	session   *managedSession
	id        QueryID
	input     *protocol.SessionQueryInput
	stream    streams.WriteStream[*protocol.SessionQueryOutput]
	timestamp time.Time
	state     QueryState
	watching  atomic.Bool
	watchers  map[uuid.UUID]func(QueryState)
	mu        sync.RWMutex
	log       logging.Logger
}

func (q *sessionQuery) ID() QueryID {
	return q.id
}

func (q *sessionQuery) Log() logging.Logger {
	return q.log
}

func (q *sessionQuery) Session() Session {
	return q.session
}

func (q *sessionQuery) Time() time.Time {
	return q.timestamp
}

func (q *sessionQuery) State() QueryState {
	return q.state
}

func (q *sessionQuery) Watch(watcher func(QueryState)) CancelFunc {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.watchers == nil {
		q.watchers = make(map[uuid.UUID]func(QueryState))
	}
	id := uuid.New()
	q.watchers[id] = watcher
	q.watching.Store(true)
	return func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		delete(q.watchers, id)
	}
}

func (q *sessionQuery) execute(input *protocol.SessionQueryInput, stream streams.WriteStream[*protocol.SessionQueryOutput]) {
	q.id = QueryID(q.session.manager.sequenceNum.Add(1))
	q.input = input
	q.stream = stream
	q.timestamp = q.session.manager.Time()
	q.log = q.session.Log().WithFields(logging.Uint64("QueryID", uint64(q.id)))
	q.state = Running
	switch input.Input.(type) {
	case *protocol.SessionQueryInput_Query:
		query := newSessionPrimitiveQuery(q)
		q.session.manager.primitives.Query(query)
	}
}

func (q *sessionQuery) Input() *protocol.SessionQueryInput {
	return q.input
}

func (q *sessionQuery) Output(output *protocol.SessionQueryOutput) {
	if q.state != Running {
		return
	}
	q.stream.Value(output)
}

func (q *sessionQuery) Error(err error) {
	if q.state != Running {
		return
	}
	q.stream.Error(err)
	q.Close()
}

func (q *sessionQuery) Cancel() {
	q.close(Canceled)
}

func (q *sessionQuery) Close() {
	q.close(Complete)
}

func (q *sessionQuery) close(phase QueryState) {
	if q.state != Running {
		return
	}
	q.state = phase
	q.stream.Close()
	if q.watching.Load() {
		q.mu.RLock()
		watchers := make([]func(QueryState), 0, len(q.watchers))
		for _, watcher := range q.watchers {
			watchers = append(watchers, watcher)
		}
		q.mu.RUnlock()
		for _, watcher := range watchers {
			watcher(phase)
		}
	}
}

var _ Query[*protocol.SessionQueryInput, *protocol.SessionQueryOutput] = (*sessionQuery)(nil)

func newSessionPrimitiveQuery(parent *sessionQuery) *sessionPrimitiveQuery {
	return &sessionPrimitiveQuery{
		sessionQuery: parent,
	}
}

type sessionPrimitiveQuery struct {
	*sessionQuery
}

func (p *sessionPrimitiveQuery) Input() *protocol.PrimitiveQueryInput {
	return p.sessionQuery.Input().GetQuery()
}

func (p *sessionPrimitiveQuery) Output(output *protocol.PrimitiveQueryOutput) {
	p.sessionQuery.Output(&protocol.SessionQueryOutput{
		Output: &protocol.SessionQueryOutput_Query{
			Query: output,
		},
	})
}

func (p *sessionPrimitiveQuery) Error(err error) {
	p.sessionQuery.Output(&protocol.SessionQueryOutput{
		Failure: getFailure(err),
	})
}

var _ Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput] = (*sessionPrimitiveQuery)(nil)
