// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
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

// ManagedCall is a proposal or query call
type ManagedCall[T CallID, I, O any] interface {
	Call[T, I, O]
	// Time returns the state machine time at the time of the call
	Time() time.Time
	// Session returns the call session
	Session() Session
	// State returns the call state
	State() CallState
	// Watch watches the call state for changes
	Watch(watcher func(CallState)) CancelFunc
	// Cancel cancels the call
	Cancel()
}

type ProposalState = CallState

type ManagedProposal[I, O any] interface {
	ManagedCall[ProposalID, I, O]
}

type QueryState = CallState

type ManagedQuery[I, O any] interface {
	ManagedCall[QueryID, I, O]
}

// ManagedProposals provides access to pending proposals
type ManagedProposals[I, O any] interface {
	// Get gets a proposal by ID
	Get(id ProposalID) (ManagedProposal[I, O], bool)
	// List lists all open proposals
	List() []ManagedProposal[I, O]
}

type SessionID uint64

type State int

const (
	Open State = iota
	Closed
)

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

type OpenSessionProposal Proposal[*protocol.OpenSessionInput, *protocol.OpenSessionOutput]
type KeepAliveProposal Proposal[*protocol.KeepAliveInput, *protocol.KeepAliveOutput]
type CloseSessionProposal Proposal[*protocol.CloseSessionInput, *protocol.CloseSessionOutput]
type SessionProposal Proposal[*protocol.SessionProposalInput, *protocol.SessionProposalOutput]
type SessionQuery Query[*protocol.SessionQueryInput, *protocol.SessionQueryOutput]

type SessionManagerContext interface {
	Context
	// Time returns the current service time
	Time() time.Time
	// Scheduler returns the service scheduler
	Scheduler() Scheduler
}

type SessionManager interface {
	Recoverable
	OpenSession(proposal OpenSessionProposal)
	KeepAlive(proposal KeepAliveProposal)
	CloseSession(proposal CloseSessionProposal)
	Propose(proposal SessionProposal)
	Query(query SessionQuery)
}

func newSessionManager(ctx SessionManagerContext, factory func(PrimitiveManagerContext) PrimitiveManager) SessionManager {
	sm := &sessionManager{
		SessionManagerContext: ctx,
		sessions:              newManagedSessions(),
		proposals:             newSessionPrimitiveProposals(),
	}
	sm.sm = factory(sm)
	return sm
}

type sessionManager struct {
	SessionManagerContext
	sm        PrimitiveManager
	sessions  *managedSessions
	proposals *sessionPrimitiveProposals
}

func (m *sessionManager) Sessions() Sessions {
	return m.sessions
}

func (m *sessionManager) Proposals() ManagedProposals[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput] {
	return m.proposals
}

func (m *sessionManager) Snapshot(writer *SnapshotWriter) error {
	sessions := m.sessions.list()
	if err := writer.WriteVarInt(len(sessions)); err != nil {
		return err
	}
	for _, session := range sessions {
		if err := session.Snapshot(writer); err != nil {
			return err
		}
	}
	return m.sm.Snapshot(writer)
}

func (m *sessionManager) Recover(reader *SnapshotReader) error {
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		session := newManagedSession(m)
		if err := session.Recover(reader); err != nil {
			return err
		}
	}
	return m.sm.Recover(reader)
}

func (m *sessionManager) OpenSession(openSession OpenSessionProposal) {
	session := newManagedSession(m)
	session.open(openSession)
}

func (m *sessionManager) KeepAlive(keepAlive KeepAliveProposal) {
	sessionID := SessionID(keepAlive.Input().SessionID)
	session, ok := m.sessions.get(sessionID)
	if !ok {
		keepAlive.Error(errors.NewForbidden("session not found"))
		keepAlive.Close()
	} else {
		session.keepAlive(keepAlive)
	}
}

func (m *sessionManager) CloseSession(closeSession CloseSessionProposal) {
	sessionID := SessionID(closeSession.Input().SessionID)
	session, ok := m.sessions.get(sessionID)
	if !ok {
		closeSession.Error(errors.NewForbidden("session not found"))
		closeSession.Close()
	} else {
		session.close(closeSession)
	}
}

func (m *sessionManager) Propose(proposal SessionProposal) {
	sessionID := SessionID(proposal.Input().SessionID)
	session, ok := m.sessions.get(sessionID)
	if !ok {
		proposal.Error(errors.NewForbidden("session not found"))
		proposal.Close()
	} else {
		session.propose(proposal)
	}
}

func (m *sessionManager) Query(query SessionQuery) {
	sessionID := SessionID(query.Input().SessionID)
	session, ok := m.sessions.get(sessionID)
	if !ok {
		query.Error(errors.NewForbidden("session not found"))
		query.Close()
	} else {
		session.query(query)
	}
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

func (s *managedSession) propose(parent Proposal[*protocol.SessionProposalInput, *protocol.SessionProposalOutput]) {
	if proposal, ok := s.proposals[parent.Input().SequenceNum]; ok {
		proposal.replay(parent)
	} else {
		proposal := newSessionProposal(s)
		s.proposals[parent.Input().SequenceNum] = proposal
		proposal.execute(parent)
	}
}

func (s *managedSession) query(parent Query[*protocol.SessionQueryInput, *protocol.SessionQueryOutput]) {
	query := newSessionQuery(s)
	query.execute(parent)
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

func (s *managedSession) open(open Proposal[*protocol.OpenSessionInput, *protocol.OpenSessionOutput]) {
	defer open.Close()
	s.id = SessionID(open.ID())
	s.state = Open
	s.lastUpdated = s.manager.Time()
	s.timeout = open.Input().Timeout
	s.log = log.WithFields(logging.Uint64("Session", uint64(s.id)))
	s.manager.sessions.add(s)
	s.scheduleExpireTimer()
	s.Log().Infow("Opened session", logging.Duration("Timeout", s.timeout))
	open.Output(&protocol.OpenSessionOutput{
		SessionID: protocol.SessionID(s.ID()),
	})
}

func (s *managedSession) keepAlive(keepAlive Proposal[*protocol.KeepAliveInput, *protocol.KeepAliveOutput]) {
	defer keepAlive.Close()

	openInputs := &bloom.BloomFilter{}
	if err := json.Unmarshal(keepAlive.Input().InputFilter, openInputs); err != nil {
		s.Log().Warn("Failed to decode request filter", err)
		keepAlive.Error(errors.NewInvalid("invalid request filter", err))
		return
	}

	s.Log().Debug("Processing keep-alive")
	for _, proposal := range s.proposals {
		if keepAlive.Input().LastInputSequenceNum < proposal.Input().SequenceNum {
			continue
		}
		sequenceNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(sequenceNumBytes, uint64(proposal.Input().SequenceNum))
		if !openInputs.Test(sequenceNumBytes) {
			proposal.Cancel()
			delete(s.proposals, proposal.Input().SequenceNum)
		} else {
			if outputSequenceNum, ok := keepAlive.Input().LastOutputSequenceNums[proposal.Input().SequenceNum]; ok {
				proposal.ack(outputSequenceNum)
			}
		}
	}

	s.queriesMu.Lock()
	for sn, query := range s.queries {
		if keepAlive.Input().LastInputSequenceNum < query.Input().SequenceNum {
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

	keepAlive.Output(&protocol.KeepAliveOutput{})

	s.lastUpdated = s.manager.Time()
	s.scheduleExpireTimer()
}

func (s *managedSession) close(close Proposal[*protocol.CloseSessionInput, *protocol.CloseSessionOutput]) {
	defer close.Close()
	s.destroy()
	close.Output(&protocol.CloseSessionOutput{})
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

func newSessionPrimitiveProposals() *sessionPrimitiveProposals {
	return &sessionPrimitiveProposals{
		proposals: make(map[ProposalID]*sessionPrimitiveProposal),
	}
}

type sessionPrimitiveProposals struct {
	proposals map[ProposalID]*sessionPrimitiveProposal
}

func (p *sessionPrimitiveProposals) Get(id ProposalID) (ManagedProposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput], bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *sessionPrimitiveProposals) List() []ManagedProposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput] {
	proposals := make([]ManagedProposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput], 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

func (p *sessionPrimitiveProposals) add(proposal *sessionPrimitiveProposal) {
	p.proposals[proposal.ID()] = proposal
}

func (p *sessionPrimitiveProposals) remove(id ProposalID) {
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
	parent       Proposal[*protocol.SessionProposalInput, *protocol.SessionProposalOutput]
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

func (p *sessionProposal) execute(parent Proposal[*protocol.SessionProposalInput, *protocol.SessionProposalOutput]) {
	p.id = parent.ID()
	p.input = parent.Input()
	p.timestamp = p.session.manager.Time()
	p.state = Running
	p.log = p.session.Log().WithFields(logging.Uint64("ProposalID", uint64(parent.ID())))
	p.parent = parent

	switch parent.Input().Input.(type) {
	case *protocol.SessionProposalInput_Proposal:
		proposal := newSessionPrimitiveProposal(p)
		p.session.manager.proposals.add(proposal)
		p.session.manager.sm.Propose(proposal)
	case *protocol.SessionProposalInput_CreatePrimitive:
		p.session.manager.sm.CreatePrimitive(newCreatePrimitiveProposal(p))
	case *protocol.SessionProposalInput_ClosePrimitive:
		p.session.manager.sm.ClosePrimitive(newClosePrimitiveProposal(p))
	}
}

func (p *sessionProposal) replay(parent Proposal[*protocol.SessionProposalInput, *protocol.SessionProposalOutput]) {
	p.parent = parent
	if p.outputs.Len() > 0 {
		p.Log().Debug("Replaying proposal outputs")
		elem := p.outputs.Front()
		for elem != nil {
			output := elem.Value.(*protocol.SessionProposalOutput)
			p.parent.Output(output)
			elem = elem.Next()
		}
	}
	if p.state == Complete {
		p.parent.Close()
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
	if p.parent != nil {
		p.parent.Output(output)
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
	if p.parent != nil {
		p.parent.Close()
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

var _ ManagedProposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput] = (*sessionPrimitiveProposal)(nil)

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
	parent    Query[*protocol.SessionQueryInput, *protocol.SessionQueryOutput]
	timestamp time.Time
	state     QueryState
	watching  atomic.Bool
	watchers  map[uuid.UUID]func(QueryState)
	mu        sync.RWMutex
	log       logging.Logger
}

func (q *sessionQuery) ID() QueryID {
	return q.parent.ID()
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

func (q *sessionQuery) execute(parent Query[*protocol.SessionQueryInput, *protocol.SessionQueryOutput]) {
	q.state = Running
	q.parent = parent
	q.timestamp = q.session.manager.Time()
	q.log = q.session.Log().WithFields(logging.Uint64("QueryID", uint64(parent.ID())))
	switch parent.Input().Input.(type) {
	case *protocol.SessionQueryInput_Query:
		query := newSessionPrimitiveQuery(q)
		q.session.manager.sm.Query(query)
	}
}

func (q *sessionQuery) Input() *protocol.SessionQueryInput {
	return q.parent.Input()
}

func (q *sessionQuery) Output(output *protocol.SessionQueryOutput) {
	if q.state != Running {
		return
	}
	q.parent.Output(output)
}

func (q *sessionQuery) Error(err error) {
	if q.state != Running {
		return
	}
	q.parent.Error(err)
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
	q.parent.Close()
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
