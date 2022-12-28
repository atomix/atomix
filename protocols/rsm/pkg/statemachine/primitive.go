// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/atomix/api/errors"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/google/uuid"
	"sync"
	"sync/atomic"
	"time"
)

type PrimitiveType[I, O any] interface {
	Name() string
	APIVersion() string
	Codec() Codec[I, O]
	NewStateMachine(PrimitiveContext[I, O]) PrimitiveStateMachine[I, O]
}

type AnyType PrimitiveType[any, any]

type NewStateMachineFunc[I, O any] func(PrimitiveContext[I, O]) PrimitiveStateMachine[I, O]

func NewPrimitiveType[I, O any](typeInfo protocol.PrimitiveType, codec Codec[I, O], factory func(PrimitiveContext[I, O]) PrimitiveStateMachine[I, O]) PrimitiveType[I, O] {
	return &primitiveType[I, O]{
		name:       typeInfo.Name,
		apiVersion: typeInfo.APIVersion,
		codec:      codec,
		factory:    factory,
	}
}

type primitiveType[I, O any] struct {
	name       string
	apiVersion string
	codec      Codec[I, O]
	factory    func(PrimitiveContext[I, O]) PrimitiveStateMachine[I, O]
}

func (t *primitiveType[I, O]) Name() string {
	return t.name
}

func (t *primitiveType[I, O]) APIVersion() string {
	return t.apiVersion
}

func (t *primitiveType[I, O]) Codec() Codec[I, O] {
	return t.codec
}

func (t *primitiveType[I, O]) NewStateMachine(context PrimitiveContext[I, O]) PrimitiveStateMachine[I, O] {
	return t.factory(context)
}

func RegisterPrimitiveType[I, O any](registry *PrimitiveTypeRegistry) func(primitiveType protocol.PrimitiveType, factory NewStateMachineFunc[I, O], codec Codec[I, O]) {
	return func(primitiveType protocol.PrimitiveType, factory NewStateMachineFunc[I, O], codec Codec[I, O]) {
		registry.register(primitiveType, func(context SessionContext, id protocol.PrimitiveID, spec protocol.PrimitiveSpec) managedPrimitive {
			return newPrimitive[I, O](context, id, spec, factory, codec)
		})
	}
}

func NewPrimitiveTypeRegistry() *PrimitiveTypeRegistry {
	return &PrimitiveTypeRegistry{
		types: make(map[protocol.PrimitiveType]func(SessionContext, protocol.PrimitiveID, protocol.PrimitiveSpec) managedPrimitive),
	}
}

type PrimitiveTypeRegistry struct {
	types map[protocol.PrimitiveType]func(SessionContext, protocol.PrimitiveID, protocol.PrimitiveSpec) managedPrimitive
	mu    sync.RWMutex
}

func (r *PrimitiveTypeRegistry) register(primitiveType protocol.PrimitiveType, factory func(SessionContext, protocol.PrimitiveID, protocol.PrimitiveSpec) managedPrimitive) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.types[primitiveType] = factory
}

func (r *PrimitiveTypeRegistry) lookup(primitiveType protocol.PrimitiveType) (func(SessionContext, protocol.PrimitiveID, protocol.PrimitiveSpec) managedPrimitive, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.types[primitiveType]
	return factory, ok
}

type PrimitiveContext[I, O any] interface {
	Context[I, O]
	Info
}

type PrimitiveStateMachine[I, O any] interface {
	Recoverable
	Propose(proposal Proposal[I, O])
	Query(query Query[I, O])
}

type PrimitiveManager interface {
	Recoverable
	CreatePrimitive(proposal CreatePrimitiveProposal)
	ClosePrimitive(proposal ClosePrimitiveProposal)
	Propose(proposal PrimitiveProposal)
	Query(query PrimitiveQuery)
}

type CreatePrimitiveProposal Proposal[*protocol.CreatePrimitiveInput, *protocol.CreatePrimitiveOutput]
type ClosePrimitiveProposal Proposal[*protocol.ClosePrimitiveInput, *protocol.ClosePrimitiveOutput]
type PrimitiveProposal Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]
type PrimitiveQuery Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput]

func newPrimitiveManager(ctx SessionContext, registry *PrimitiveTypeRegistry) PrimitiveManager {
	return &primitiveManager{
		SessionContext: ctx,
		registry:       registry,
		primitives:     make(map[protocol.PrimitiveID]managedPrimitive),
	}
}

type primitiveManager struct {
	SessionContext
	registry   *PrimitiveTypeRegistry
	primitives map[protocol.PrimitiveID]managedPrimitive
}

func (m *primitiveManager) Snapshot(writer *SnapshotWriter) error {
	if err := writer.WriteVarInt(len(m.primitives)); err != nil {
		return err
	}
	for _, primitive := range m.primitives {
		snapshot := &protocol.PrimitiveSnapshot{
			PrimitiveID: primitive.ID(),
			Spec:        primitive.Spec(),
		}
		if err := writer.WriteMessage(snapshot); err != nil {
			return err
		}
		if err := primitive.Snapshot(writer); err != nil {
			return err
		}
	}
	return nil
}

func (m *primitiveManager) Recover(reader *SnapshotReader) error {
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		snapshot := &protocol.PrimitiveSnapshot{}
		if err := reader.ReadMessage(snapshot); err != nil {
			return err
		}
		factory, ok := m.registry.lookup(snapshot.Spec.Type)
		if !ok {
			return errors.NewFault("primitive type not found")
		}
		primitiveID := snapshot.PrimitiveID
		primitive := factory(m.SessionContext, primitiveID, snapshot.Spec)
		m.primitives[primitiveID] = primitive
		if err := primitive.Recover(reader); err != nil {
			return err
		}
	}
	return nil
}

func (m *primitiveManager) Propose(proposal PrimitiveProposal) {
	primitive, ok := m.primitives[proposal.Input().PrimitiveID]
	if !ok {
		proposal.Error(errors.NewForbidden("primitive %d not found", proposal.Input().PrimitiveID))
		proposal.Close()
	} else {
		primitive.propose(proposal)
	}
}

func (m *primitiveManager) CreatePrimitive(proposal CreatePrimitiveProposal) {
	var primitive managedPrimitive
	for _, p := range m.primitives {
		if p.Spec().Namespace == proposal.Input().Namespace &&
			p.Spec().Name == proposal.Input().Name {
			if p.Spec().Type.Name != proposal.Input().Type.Name {
				proposal.Error(errors.NewForbidden("cannot create primitive of a different type with the same name"))
				proposal.Close()
				return
			}
			if p.Spec().Type.APIVersion != proposal.Input().Type.APIVersion {
				proposal.Error(errors.NewForbidden("cannot create primitive of a different API version with the same type and name"))
				proposal.Close()
				return
			}
			primitive = p
			break
		}
	}

	if primitive == nil {
		factory, ok := m.registry.lookup(proposal.Input().Type)
		if !ok {
			proposal.Error(errors.NewForbidden("unknown primitive type"))
			proposal.Close()
			return
		} else {
			primitiveID := protocol.PrimitiveID(proposal.ID())
			primitive = factory(m.SessionContext, primitiveID, proposal.Input().PrimitiveSpec)
			m.primitives[primitiveID] = primitive
		}
	}

	primitive.open(proposal)
}

func (m *primitiveManager) ClosePrimitive(proposal ClosePrimitiveProposal) {
	primitive, ok := m.primitives[proposal.Input().PrimitiveID]
	if !ok {
		proposal.Error(errors.NewForbidden("primitive %d not found", proposal.Input().PrimitiveID))
		proposal.Close()
	} else {
		primitive.close(proposal)
	}
}

func (m *primitiveManager) Query(query PrimitiveQuery) {
	primitive, ok := m.primitives[query.Input().PrimitiveID]
	if !ok {
		query.Error(errors.NewForbidden("primitive %d not found", query.Input().PrimitiveID))
		query.Close()
	} else {
		primitive.query(query)
	}
}

type Info interface {
	// ID returns the service identifier
	ID() protocol.PrimitiveID
	// Log returns the service logger
	Log() logging.Logger
	// Spec returns the primitive spec
	Spec() protocol.PrimitiveSpec
}

type managedPrimitive interface {
	Recoverable
	Info
	open(proposal Proposal[*protocol.CreatePrimitiveInput, *protocol.CreatePrimitiveOutput])
	close(proposal Proposal[*protocol.ClosePrimitiveInput, *protocol.ClosePrimitiveOutput])
	propose(proposal Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput])
	query(query Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput])
}

func newPrimitiveContext[I, O any](parent SessionContext, id protocol.PrimitiveID, spec protocol.PrimitiveSpec, codec Codec[I, O]) *primitiveContext[I, O] {
	return &primitiveContext[I, O]{
		SessionContext: parent,
		id:             id,
		spec:           spec,
		sessions:       newPrimitiveSessions[I, O](),
		proposals:      newPrimitiveProposals[I, O](),
		codec:          codec,
		log: log.WithFields(
			logging.String("Type", spec.Type.Name),
			logging.String("APIVersion", spec.Type.APIVersion),
			logging.Uint64("ID", uint64(id)),
			logging.String("Namespace", spec.Namespace),
			logging.String("Name", spec.Name)),
	}
}

type primitiveContext[I, O any] struct {
	SessionContext
	id        protocol.PrimitiveID
	spec      protocol.PrimitiveSpec
	codec     Codec[I, O]
	sessions  *primitiveSessions[I, O]
	proposals *primitiveProposals[I, O]
	log       logging.Logger
}

func (c *primitiveContext[I, O]) Log() logging.Logger {
	return c.log
}

func (c *primitiveContext[I, O]) ID() protocol.PrimitiveID {
	return c.id
}

func (c *primitiveContext[I, O]) Spec() protocol.PrimitiveSpec {
	return c.spec
}

func (c *primitiveContext[I, O]) Sessions() Sessions {
	return c.sessions
}

func (c *primitiveContext[I, O]) Proposals() Proposals[I, O] {
	return c.proposals
}

func newPrimitive[I, O any](parent SessionContext, id protocol.PrimitiveID, spec protocol.PrimitiveSpec, factory NewStateMachineFunc[I, O], codec Codec[I, O]) managedPrimitive {
	context := newPrimitiveContext[I, O](parent, id, spec, codec)
	return &primitiveExecutor[I, O]{
		primitiveContext: context,
		sm:               factory(context),
	}
}

type primitiveExecutor[I, O any] struct {
	*primitiveContext[I, O]
	log logging.Logger
	sm  PrimitiveStateMachine[I, O]
}

func (p *primitiveExecutor[I, O]) Snapshot(writer *SnapshotWriter) error {
	if err := writer.WriteVarInt(len(p.sessions.sessions)); err != nil {
		return err
	}
	for _, session := range p.sessions.list() {
		if err := session.Snapshot(writer); err != nil {
			return err
		}
	}
	return p.sm.Snapshot(writer)
}

func (p *primitiveExecutor[I, O]) Recover(reader *SnapshotReader) error {
	n, err := reader.ReadVarInt()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		session := newPrimitiveSession[I, O](p)
		if err := session.Recover(reader); err != nil {
			return err
		}
	}
	return p.sm.Recover(reader)
}

func (p *primitiveExecutor[I, O]) open(proposal Proposal[*protocol.CreatePrimitiveInput, *protocol.CreatePrimitiveOutput]) {
	session := newPrimitiveSession[I, O](p)
	session.open(proposal.Session())
	proposal.Output(&protocol.CreatePrimitiveOutput{
		PrimitiveID: p.ID(),
	})
	proposal.Close()
}

func (p *primitiveExecutor[I, O]) close(proposal Proposal[*protocol.ClosePrimitiveInput, *protocol.ClosePrimitiveOutput]) {
	session, ok := p.sessions.get(proposal.Session().ID())
	if !ok {
		proposal.Error(errors.NewForbidden("session not found"))
		proposal.Close()
	} else {
		session.close()
		proposal.Output(&protocol.ClosePrimitiveOutput{})
		proposal.Close()
	}
}

func (p *primitiveExecutor[I, O]) propose(proposal Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) {
	session, ok := p.sessions.get(proposal.Session().ID())
	if !ok {
		proposal.Error(errors.NewForbidden("session not found"))
		proposal.Close()
	} else {
		session.propose(proposal)
	}
}

func (p *primitiveExecutor[I, O]) query(query Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput]) {
	session, ok := p.sessions.get(query.Session().ID())
	if !ok {
		query.Error(errors.NewForbidden("session not found"))
		query.Close()
	} else {
		session.query(query)
	}
}

func newPrimitiveSession[I, O any](primitive *primitiveExecutor[I, O]) *primitiveSession[I, O] {
	s := &primitiveSession[I, O]{
		primitive: primitive,
		proposals: make(map[ProposalID]*primitiveProposal[I, O]),
		queries:   make(map[QueryID]*primitiveQuery[I, O]),
		watchers:  make(map[uuid.UUID]func(State)),
	}
	return s
}

type primitiveSession[I, O any] struct {
	primitive *primitiveExecutor[I, O]
	parent    Session
	proposals map[ProposalID]*primitiveProposal[I, O]
	queries   map[QueryID]*primitiveQuery[I, O]
	queriesMu sync.Mutex
	state     State
	watchers  map[uuid.UUID]func(State)
	cancel    CancelFunc
	log       logging.Logger
}

func (s *primitiveSession[I, O]) Log() logging.Logger {
	return s.log
}

func (s *primitiveSession[I, O]) ID() SessionID {
	return s.parent.ID()
}

func (s *primitiveSession[I, O]) State() State {
	return s.state
}

func (s *primitiveSession[I, O]) Watch(watcher func(State)) CancelFunc {
	id := uuid.New()
	s.watchers[id] = watcher
	return func() {
		delete(s.watchers, id)
	}
}

func (s *primitiveSession[I, O]) Snapshot(writer *SnapshotWriter) error {
	if err := writer.WriteVarUint64(uint64(s.ID())); err != nil {
		return err
	}
	return nil
}

func (s *primitiveSession[I, O]) Recover(reader *SnapshotReader) error {
	sessionID, err := reader.ReadVarUint64()
	if err != nil {
		return err
	}
	parent, ok := s.primitive.SessionContext.Sessions().Get(SessionID(sessionID))
	if !ok {
		return errors.NewFault("session not found")
	}
	s.open(parent)
	for _, sessionProposal := range s.primitive.SessionContext.Proposals().List() {
		if sessionProposal.Input().PrimitiveID == s.primitive.ID() {
			proposal := newPrimitiveProposal[I, O](s)
			if proposal.init(sessionProposal) {
				s.registerProposal(proposal)
			}
		}
	}
	return nil
}

func (s *primitiveSession[I, O]) registerProposal(proposal *primitiveProposal[I, O]) {
	s.proposals[proposal.ID()] = proposal
}

func (s *primitiveSession[I, O]) unregisterProposal(proposalID ProposalID) {
	delete(s.proposals, proposalID)
}

func (s *primitiveSession[I, O]) registerQuery(query *primitiveQuery[I, O]) {
	s.queriesMu.Lock()
	s.queries[query.ID()] = query
	s.queriesMu.Unlock()
}

func (s *primitiveSession[I, O]) unregisterQuery(queryID QueryID) {
	s.queriesMu.Lock()
	delete(s.queries, queryID)
	s.queriesMu.Unlock()
}

func (s *primitiveSession[I, O]) open(parent Session) {
	s.parent = parent
	s.state = Open
	s.log = s.primitive.Log().WithFields(logging.Uint64("SessionID", uint64(parent.ID())))
	s.cancel = parent.Watch(func(state State) {
		if state == Closed {
			s.close()
		}
	})
	s.primitive.sessions.add(s)
}

func (s *primitiveSession[I, O]) propose(parent Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) {
	proposal := newPrimitiveProposal[I, O](s)
	proposal.execute(parent)
}

func (s *primitiveSession[I, O]) query(parent Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput]) {
	query := newPrimitiveQuery[I, O](s)
	query.execute(parent)
}

func (s *primitiveSession[I, O]) close() {
	s.cancel()
	s.primitive.sessions.remove(s.ID())
	s.state = Closed
	for _, proposal := range s.proposals {
		proposal.Cancel()
	}
	for _, query := range s.queries {
		query.Cancel()
	}
	for _, watcher := range s.watchers {
		watcher(Closed)
	}
}

var _ Session = (*primitiveSession[any, any])(nil)

func newPrimitiveSessions[I, O any]() *primitiveSessions[I, O] {
	return &primitiveSessions[I, O]{
		sessions: make(map[SessionID]*primitiveSession[I, O]),
	}
}

type primitiveSessions[I, O any] struct {
	sessions map[SessionID]*primitiveSession[I, O]
}

func (s *primitiveSessions[I, O]) Get(id SessionID) (Session, bool) {
	session, ok := s.sessions[id]
	return session, ok
}

func (s *primitiveSessions[I, O]) List() []Session {
	sessions := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func (s *primitiveSessions[I, O]) add(session *primitiveSession[I, O]) {
	s.sessions[session.ID()] = session
}

func (s *primitiveSessions[I, O]) remove(sessionID SessionID) bool {
	if _, ok := s.sessions[sessionID]; ok {
		delete(s.sessions, sessionID)
		return true
	}
	return false
}

func (s *primitiveSessions[I, O]) get(id SessionID) (*primitiveSession[I, O], bool) {
	session, ok := s.sessions[id]
	return session, ok
}

func (s *primitiveSessions[I, O]) list() []*primitiveSession[I, O] {
	sessions := make([]*primitiveSession[I, O], 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func newPrimitiveProposal[I, O any](session *primitiveSession[I, O]) *primitiveProposal[I, O] {
	return &primitiveProposal[I, O]{
		session: session,
	}
}

type primitiveProposal[I, O any] struct {
	parent   Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]
	session  *primitiveSession[I, O]
	input    I
	state    CallState
	watchers map[uuid.UUID]func(CallState)
	cancel   CancelFunc
	log      logging.Logger
}

func (p *primitiveProposal[I, O]) ID() ProposalID {
	return p.parent.ID()
}

func (p *primitiveProposal[I, O]) Log() logging.Logger {
	return p.log
}

func (p *primitiveProposal[I, O]) Time() time.Time {
	return p.parent.Time()
}

func (p *primitiveProposal[I, O]) State() ProposalState {
	return p.state
}

func (p *primitiveProposal[I, O]) Watch(watcher func(ProposalState)) CancelFunc {
	if p.state != Running {
		watcher(p.state)
		return func() {}
	}
	if p.watchers == nil {
		p.watchers = make(map[uuid.UUID]func(ProposalState))
	}
	id := uuid.New()
	p.watchers[id] = watcher
	return func() {
		delete(p.watchers, id)
	}
}

func (p *primitiveProposal[I, O]) Session() Session {
	return p.session
}

func (p *primitiveProposal[I, O]) init(parent Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) bool {
	input, err := p.session.primitive.codec.DecodeInput(parent.Input().Payload)
	if err != nil {
		p.Log().Errorw("Failed decoding proposal", logging.Error("Error", err))
		parent.Error(errors.NewInternal("failed decoding proposal: %s", err.Error()))
		parent.Close()
		return false
	}

	p.parent = parent
	p.input = input
	p.state = Running
	p.log = p.session.Log().WithFields(logging.Uint64("ProposalID", uint64(parent.ID())))
	p.cancel = parent.Watch(func(state ProposalState) {
		if p.state != Running {
			return
		}
		switch state {
		case Complete:
			p.destroy(Complete)
		case Canceled:
			p.destroy(Canceled)
		}
	})
	p.session.primitive.proposals.add(p)
	return true
}

func (p *primitiveProposal[I, O]) execute(parent Proposal[*protocol.PrimitiveProposalInput, *protocol.PrimitiveProposalOutput]) {
	if p.state != Pending {
		return
	}

	if p.init(parent) {
		p.session.primitive.sm.Propose(p)
		if p.state == Running {
			p.session.registerProposal(p)
		}
	}
}

func (p *primitiveProposal[I, O]) Input() I {
	return p.input
}

func (p *primitiveProposal[I, O]) Output(output O) {
	if p.state != Running {
		return
	}
	payload, err := p.session.primitive.codec.EncodeOutput(output)
	if err != nil {
		p.Log().Errorw("Failed encoding proposal", logging.Error("Error", err))
		p.parent.Error(errors.NewInternal("failed encoding proposal: %s", err.Error()))
		p.parent.Close()
	} else {
		p.parent.Output(&protocol.PrimitiveProposalOutput{
			Payload: payload,
		})
	}
}

func (p *primitiveProposal[I, O]) Error(err error) {
	if p.state != Running {
		return
	}
	p.parent.Error(err)
	p.parent.Close()
}

func (p *primitiveProposal[I, O]) Cancel() {
	if p.state != Running {
		return
	}
	p.parent.Cancel()
}

func (p *primitiveProposal[I, O]) Close() {
	if p.state != Running {
		return
	}
	p.parent.Close()
}

func (p *primitiveProposal[I, O]) destroy(state ProposalState) {
	p.cancel()
	p.state = state
	p.session.primitive.proposals.remove(p.ID())
	p.session.unregisterProposal(p.ID())
	if p.watchers != nil {
		for _, watcher := range p.watchers {
			watcher(state)
		}
	}
}

var _ Proposal[any, any] = (*primitiveProposal[any, any])(nil)

func newPrimitiveProposals[I, O any]() *primitiveProposals[I, O] {
	return &primitiveProposals[I, O]{
		proposals: make(map[ProposalID]*primitiveProposal[I, O]),
	}
}

type primitiveProposals[I, O any] struct {
	proposals map[ProposalID]*primitiveProposal[I, O]
}

func (p *primitiveProposals[I, O]) Get(id ProposalID) (Proposal[I, O], bool) {
	proposal, ok := p.proposals[id]
	return proposal, ok
}

func (p *primitiveProposals[I, O]) List() []Proposal[I, O] {
	proposals := make([]Proposal[I, O], 0, len(p.proposals))
	for _, proposal := range p.proposals {
		proposals = append(proposals, proposal)
	}
	return proposals
}

func (p *primitiveProposals[I, O]) add(proposal *primitiveProposal[I, O]) {
	p.proposals[proposal.ID()] = proposal
}

func (p *primitiveProposals[I, O]) remove(id ProposalID) {
	delete(p.proposals, id)
}

var _ Proposals[any, any] = (*primitiveProposals[any, any])(nil)

func newPrimitiveQuery[I, O any](session *primitiveSession[I, O]) *primitiveQuery[I, O] {
	return &primitiveQuery[I, O]{
		session: session,
	}
}

type primitiveQuery[I, O any] struct {
	parent     Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput]
	session    *primitiveSession[I, O]
	input      I
	state      CallState
	watchers   map[uuid.UUID]func(CallState)
	registered atomic.Bool
	cancel     CancelFunc
	log        logging.Logger
}

func (q *primitiveQuery[I, O]) ID() QueryID {
	return q.parent.ID()
}

func (q *primitiveQuery[I, O]) Log() logging.Logger {
	return q.log
}

func (q *primitiveQuery[I, O]) Time() time.Time {
	return q.parent.Time()
}

func (q *primitiveQuery[I, O]) State() QueryState {
	return q.state
}

func (q *primitiveQuery[I, O]) Watch(watcher func(QueryState)) CancelFunc {
	if q.state != Running {
		watcher(q.state)
		return func() {}
	}
	if q.watchers == nil {
		q.watchers = make(map[uuid.UUID]func(QueryState))
	}
	id := uuid.New()
	q.watchers[id] = watcher
	return func() {
		delete(q.watchers, id)
	}
}

func (q *primitiveQuery[I, O]) Session() Session {
	return q.session
}

func (q *primitiveQuery[I, O]) init(parent Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput]) error {
	input, err := q.session.primitive.codec.DecodeInput(parent.Input().Payload)
	if err != nil {
		return err
	}

	q.parent = parent
	q.input = input
	q.state = Running
	q.log = q.session.Log().WithFields(logging.Uint64("QueryID", uint64(parent.ID())))
	q.cancel = parent.Watch(func(state QueryState) {
		if q.state != Running {
			return
		}
		switch state {
		case Complete:
			q.destroy(Complete)
		case Canceled:
			q.destroy(Canceled)
		}
	})
	return nil
}

func (q *primitiveQuery[I, O]) execute(parent Query[*protocol.PrimitiveQueryInput, *protocol.PrimitiveQueryOutput]) {
	if q.state != Pending {
		return
	}

	if err := q.init(parent); err != nil {
		q.Log().Errorw("Failed decoding proposal", logging.Error("Error", err))
		parent.Error(errors.NewInternal("failed decoding proposal: %s", err.Error()))
		parent.Close()
	} else {
		q.session.primitive.sm.Query(q)
		if q.state == Running {
			q.session.registerQuery(q)
			q.registered.Store(true)
		}
	}
}

func (q *primitiveQuery[I, O]) Input() I {
	return q.input
}

func (q *primitiveQuery[I, O]) Output(output O) {
	if q.state != Running {
		return
	}
	payload, err := q.session.primitive.codec.EncodeOutput(output)
	if err != nil {
		q.Log().Errorw("Failed encoding proposal", logging.Error("Error", err))
		q.parent.Error(errors.NewInternal("failed encoding proposal: %s", err.Error()))
		q.parent.Close()
	} else {
		q.parent.Output(&protocol.PrimitiveQueryOutput{
			Payload: payload,
		})
	}
}

func (q *primitiveQuery[I, O]) Error(err error) {
	if q.state != Running {
		return
	}
	q.parent.Error(err)
	q.parent.Close()
}

func (q *primitiveQuery[I, O]) Cancel() {
	if q.state != Running {
		return
	}
	q.parent.Cancel()
}

func (q *primitiveQuery[I, O]) Close() {
	if q.state != Running {
		return
	}
	q.parent.Close()
}

func (q *primitiveQuery[I, O]) destroy(state QueryState) {
	q.cancel()
	q.state = state
	if q.registered.Load() {
		q.session.unregisterQuery(q.ID())
	}
	if q.watchers != nil {
		for _, watcher := range q.watchers {
			watcher(state)
		}
	}
}

var _ Query[any, any] = (*primitiveQuery[any, any])(nil)
