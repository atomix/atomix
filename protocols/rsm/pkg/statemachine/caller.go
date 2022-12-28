// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/gogo/protobuf/proto"
	"time"
)

type Caller[T Call[U, I, O], U CallID, I, O any] func(T)

type Proposer[I1, O1, I2, O2 any] Caller[Proposal[I1, O1], ProposalID, I1, O1]

func NewProposals[I1, O1, I2, O2 proto.Message](ctx PrimitiveContext[I1, O1]) *ProposalsBuilder[I1, O1, I2, O2] {
	return &ProposalsBuilder[I1, O1, I2, O2]{
		ctx: ctx,
	}
}

type ProposalsBuilder[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (b *ProposalsBuilder[I1, O1, I2, O2]) Decoder(f func(I1) (I2, bool)) *ProposalsBuilder[I1, O1, I2, O2] {
	b.decoder = f
	return b
}

func (b *ProposalsBuilder[I1, O1, I2, O2]) Encoder(f func(O2) O1) *ProposalsBuilder[I1, O1, I2, O2] {
	b.encoder = f
	return b
}

func (b *ProposalsBuilder[I1, O1, I2, O2]) Build() Proposals[I2, O2] {
	return &transcodingProposals[I1, O1, I2, O2]{
		parent:  b.ctx.Proposals(),
		decoder: b.decoder,
		encoder: b.encoder,
	}
}

func NewProposer[I1, O1, I2, O2 proto.Message](name string) *ProposerBuilder[I1, O1, I2, O2] {
	return &ProposerBuilder[I1, O1, I2, O2]{
		name: name,
	}
}

type ProposerBuilder[I1, O1, I2, O2 proto.Message] struct {
	name    string
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Decoder(f func(I1) (I2, bool)) *ProposerBuilder[I1, O1, I2, O2] {
	b.decoder = f
	return b
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Encoder(f func(O2) O1) *ProposerBuilder[I1, O1, I2, O2] {
	b.encoder = f
	return b
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Build(f func(Proposal[I2, O2])) Proposer[I1, O1, I2, O2] {
	return func(parent Proposal[I1, O1]) {
		input, ok := b.decoder(parent.Input())
		if !ok {
			return
		}
		proposal := newTranscodingProposal[I1, O1, I2, O2](
			parent, input, b.decoder, b.encoder, parent.Log().WithFields(logging.String("Method", b.name)))
		proposal.Log().Debugw("Applying proposal", logging.Stringer("Input", proposal.Input()))
		f(proposal)
	}
}

type Querier[I1, O1, I2, O2 any] Caller[Query[I1, O1], QueryID, I1, O1]

func NewQuerier[I1, O1, I2, O2 proto.Message](name string) *QuerierBuilder[I1, O1, I2, O2] {
	return &QuerierBuilder[I1, O1, I2, O2]{
		name: name,
	}
}

type QuerierBuilder[I1, O1, I2, O2 proto.Message] struct {
	name    string
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Decoder(f func(I1) (I2, bool)) *QuerierBuilder[I1, O1, I2, O2] {
	b.decoder = f
	return b
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Encoder(f func(O2) O1) *QuerierBuilder[I1, O1, I2, O2] {
	b.encoder = f
	return b
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Build(f func(Query[I2, O2])) Querier[I1, O1, I2, O2] {
	return func(parent Query[I1, O1]) {
		input, ok := b.decoder(parent.Input())
		if !ok {
			return
		}
		query := newTranscodingQuery[I1, O1, I2, O2](
			parent, input, b.decoder, b.encoder, parent.Log().WithFields(logging.String("Method", b.name)))
		query.Log().Debugw("Applying query", logging.Stringer("Input", query.Input()))
		f(query)
	}
}

type transcodingProposer[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
	name    string
	f       func(Proposal[I2, O2])
}

func (p *transcodingProposer[I1, O1, I2, O2]) Proposals() Proposals[I2, O2] {
	return newTranscodingProposals[I1, O1, I2, O2](p.ctx.Proposals(), p.decoder, p.encoder)
}

func (p *transcodingProposer[I1, O1, I2, O2]) Call(parent Proposal[I1, O1]) {
	input, ok := p.decoder(parent.Input())
	if !ok {
		return
	}
	proposal := newTranscodingProposal[I1, O1, I2, O2](parent, input, p.decoder, p.encoder, parent.Log().WithFields(logging.String("Method", p.name)))
	proposal.Log().Debugw("Applying proposal", logging.Stringer("Input", proposal.Input()))
	p.f(proposal)
}

type transcodingQuerier[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
	name    string
	f       func(Query[I2, O2])
}

func (q *transcodingQuerier[I1, O1, I2, O2]) Call(parent Query[I1, O1]) {
	input, ok := q.decoder(parent.Input())
	if !ok {
		return
	}
	query := newTranscodingQuery[I1, O1, I2, O2](parent, input, q.decoder, q.encoder, parent.Log().WithFields(logging.String("Method", q.name)))
	query.Log().Debugw("Applying query", logging.Stringer("Input", query.Input()))
	q.f(query)
}

func newTranscodingProposals[I1, O1, I2, O2 any](parent Proposals[I1, O1], decoder func(I1) (I2, bool), encoder func(O2) O1) Proposals[I2, O2] {
	return &transcodingProposals[I1, O1, I2, O2]{
		parent:  parent,
		decoder: decoder,
		encoder: encoder,
	}
}

type transcodingProposals[I1, O1, I2, O2 any] struct {
	parent  Proposals[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (p *transcodingProposals[I1, O1, I2, O2]) Get(id ProposalID) (Proposal[I2, O2], bool) {
	parent, ok := p.parent.Get(id)
	if !ok {
		return nil, false
	}
	if input, ok := p.decoder(parent.Input()); ok {
		return newTranscodingProposal[I1, O1, I2, O2](parent, input, p.decoder, p.encoder, parent.Log()), true
	}
	return nil, false
}

func (p *transcodingProposals[I1, O1, I2, O2]) List() []Proposal[I2, O2] {
	parents := p.parent.List()
	proposals := make([]Proposal[I2, O2], 0, len(parents))
	for _, parent := range parents {
		if input, ok := p.decoder(parent.Input()); ok {
			proposal := newTranscodingProposal[I1, O1, I2, O2](parent, input, p.decoder, p.encoder, parent.Log())
			proposals = append(proposals, proposal)
		}
	}
	return proposals
}

func newTranscodingCall[T CallID, I1, O1, I2, O2 any](
	parent Call[T, I1, O1],
	input I2,
	decoder func(I1) (I2, bool),
	encoder func(O2) O1,
	log logging.Logger) Call[T, I2, O2] {
	return &transcodingCall[T, I1, O1, I2, O2]{
		parent:  parent,
		input:   input,
		decoder: decoder,
		encoder: encoder,
		log:     log,
	}
}

type transcodingCall[T CallID, I1, O1, I2, O2 any] struct {
	parent  Call[T, I1, O1]
	input   I2
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
	log     logging.Logger
}

func (p *transcodingCall[T, I1, O1, I2, O2]) ID() T {
	return p.parent.ID()
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Log() logging.Logger {
	return p.log
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Time() time.Time {
	return p.parent.Time()
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Session() Session {
	return p.parent.Session()
}

func (p *transcodingCall[T, I1, O1, I2, O2]) State() CallState {
	return p.parent.State()
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Watch(watcher func(state CallState)) CancelFunc {
	return p.parent.Watch(watcher)
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Input() I2 {
	return p.input
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Output(output O2) {
	p.parent.Output(p.encoder(output))
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Error(err error) {
	p.parent.Error(err)
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Cancel() {
	p.parent.Cancel()
}

func (p *transcodingCall[T, I1, O1, I2, O2]) Close() {
	p.parent.Close()
}

func newTranscodingProposal[I1, O1, I2, O2 any](
	parent Proposal[I1, O1],
	input I2,
	decoder func(I1) (I2, bool),
	encoder func(O2) O1,
	log logging.Logger) Proposal[I2, O2] {
	return newTranscodingCall[ProposalID, I1, O1, I2, O2](parent, input, decoder, encoder, log)
}

func newTranscodingQuery[I1, O1, I2, O2 any](
	parent Query[I1, O1],
	input I2,
	decoder func(I1) (I2, bool),
	encoder func(O2) O1,
	log logging.Logger) Query[I2, O2] {
	return newTranscodingCall[QueryID, I1, O1, I2, O2](parent, input, decoder, encoder, log)
}
