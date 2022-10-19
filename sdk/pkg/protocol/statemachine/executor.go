// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"github.com/gogo/protobuf/proto"
	"time"
)

type Caller[T ManagedCall[U, I, O], U CallID, I, O any] interface {
	Call(T)
}

type Proposer[I1, O1, I2, O2 any] interface {
	Caller[ManagedProposal[I1, O1], ProposalID, I1, O1]
	Proposals() ManagedProposals[I2, O2]
}

func NewProposer[I1, O1, I2, O2 proto.Message](ctx PrimitiveContext[I1, O1]) *ProposerBuilder[I1, O1, I2, O2] {
	return &ProposerBuilder[I1, O1, I2, O2]{
		ctx: ctx,
	}
}

type ProposerBuilder[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	name    string
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Name(name string) *ProposerBuilder[I1, O1, I2, O2] {
	b.name = name
	return b
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Decoder(f func(I1) (I2, bool)) *ProposerBuilder[I1, O1, I2, O2] {
	b.decoder = f
	return b
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Encoder(f func(O2) O1) *ProposerBuilder[I1, O1, I2, O2] {
	b.encoder = f
	return b
}

func (b *ProposerBuilder[I1, O1, I2, O2]) Build(f func(ManagedProposal[I2, O2])) Proposer[I1, O1, I2, O2] {
	return &transcodingProposer[I1, O1, I2, O2]{
		ctx:     b.ctx,
		decoder: b.decoder,
		encoder: b.encoder,
		name:    b.name,
		f:       f,
	}
}

var _ CallerBuilder[
	ManagedProposal[proto.Message, proto.Message],
	ProposalID,
	proto.Message,
	proto.Message,
	Proposer[proto.Message, proto.Message, proto.Message, proto.Message]] = (*ProposerBuilder[proto.Message, proto.Message, proto.Message, proto.Message])(nil)

type CallerBuilder[
	T ManagedCall[U, I, O],
	U CallID,
	I proto.Message,
	O proto.Message,
	E Caller[T, U, I, O]] interface {
	Build(f func(T)) E
}

type Querier[I1, O1, I2, O2 any] interface {
	Caller[ManagedQuery[I1, O1], QueryID, I1, O1]
}

func NewQuerier[I1, O1, I2, O2 proto.Message](ctx PrimitiveContext[I1, O1]) *QuerierBuilder[I1, O1, I2, O2] {
	return &QuerierBuilder[I1, O1, I2, O2]{
		ctx: ctx,
	}
}

type QuerierBuilder[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	name    string
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Name(name string) *QuerierBuilder[I1, O1, I2, O2] {
	b.name = name
	return b
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Decoder(f func(I1) (I2, bool)) *QuerierBuilder[I1, O1, I2, O2] {
	b.decoder = f
	return b
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Encoder(f func(O2) O1) *QuerierBuilder[I1, O1, I2, O2] {
	b.encoder = f
	return b
}

func (b *QuerierBuilder[I1, O1, I2, O2]) Build(f func(ManagedQuery[I2, O2])) Querier[I1, O1, I2, O2] {
	return &transcodingQuerier[I1, O1, I2, O2]{
		ctx:     b.ctx,
		decoder: b.decoder,
		encoder: b.encoder,
		name:    b.name,
		f:       f,
	}
}

var _ CallerBuilder[
	ManagedQuery[proto.Message, proto.Message],
	QueryID,
	proto.Message,
	proto.Message,
	Querier[proto.Message, proto.Message, proto.Message, proto.Message]] = (*QuerierBuilder[proto.Message, proto.Message, proto.Message, proto.Message])(nil)

type transcodingProposer[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
	name    string
	f       func(ManagedProposal[I2, O2])
}

func (p *transcodingProposer[I1, O1, I2, O2]) Proposals() ManagedProposals[I2, O2] {
	return newTranscodingProposals[I1, O1, I2, O2](p.ctx.Proposals(), p.decoder, p.encoder)
}

func (p *transcodingProposer[I1, O1, I2, O2]) Call(parent ManagedProposal[I1, O1]) {
	input, ok := p.decoder(parent.Input())
	if !ok {
		return
	}
	proposal := newTranscodingProposal[I1, O1, I2, O2](parent, input, p.decoder, p.encoder, parent.Log().WithFields(logging.String("Method", p.name)))
	proposal.Log().Debugw("Applying proposal", logging.Stringer("Input", stringer.Truncate(proposal.Input(), truncLen)))
	p.f(proposal)
}

type transcodingQuerier[I1, O1, I2, O2 proto.Message] struct {
	ctx     PrimitiveContext[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
	name    string
	f       func(ManagedQuery[I2, O2])
}

func (q *transcodingQuerier[I1, O1, I2, O2]) Call(parent ManagedQuery[I1, O1]) {
	input, ok := q.decoder(parent.Input())
	if !ok {
		return
	}
	query := newTranscodingQuery[I1, O1, I2, O2](parent, input, q.decoder, q.encoder, parent.Log().WithFields(logging.String("Method", q.name)))
	query.Log().Debugw("Applying query", logging.Stringer("Input", stringer.Truncate(query.Input(), truncLen)))
	q.f(query)
}

func newTranscodingProposals[I1, O1, I2, O2 any](parent ManagedProposals[I1, O1], decoder func(I1) (I2, bool), encoder func(O2) O1) ManagedProposals[I2, O2] {
	return &transcodingProposals[I1, O1, I2, O2]{
		parent:  parent,
		decoder: decoder,
		encoder: encoder,
	}
}

type transcodingProposals[I1, O1, I2, O2 any] struct {
	parent  ManagedProposals[I1, O1]
	decoder func(I1) (I2, bool)
	encoder func(O2) O1
}

func (p *transcodingProposals[I1, O1, I2, O2]) Get(id ProposalID) (ManagedProposal[I2, O2], bool) {
	parent, ok := p.parent.Get(id)
	if !ok {
		return nil, false
	}
	if input, ok := p.decoder(parent.Input()); ok {
		return newTranscodingProposal[I1, O1, I2, O2](parent, input, p.decoder, p.encoder, parent.Log()), true
	}
	return nil, false
}

func (p *transcodingProposals[I1, O1, I2, O2]) List() []ManagedProposal[I2, O2] {
	parents := p.parent.List()
	proposals := make([]ManagedProposal[I2, O2], 0, len(parents))
	for _, parent := range parents {
		if input, ok := p.decoder(parent.Input()); ok {
			proposal := newTranscodingProposal[I1, O1, I2, O2](parent, input, p.decoder, p.encoder, parent.Log())
			proposals = append(proposals, proposal)
		}
	}
	return proposals
}

func newTranscodingCall[T CallID, I1, O1, I2, O2 any](
	parent ManagedCall[T, I1, O1],
	input I2,
	decoder func(I1) (I2, bool),
	encoder func(O2) O1,
	log logging.Logger) ManagedCall[T, I2, O2] {
	return &transcodingCall[T, I1, O1, I2, O2]{
		parent:  parent,
		input:   input,
		decoder: decoder,
		encoder: encoder,
		log:     log,
	}
}

type transcodingCall[T CallID, I1, O1, I2, O2 any] struct {
	parent  ManagedCall[T, I1, O1]
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
	parent ManagedProposal[I1, O1],
	input I2,
	decoder func(I1) (I2, bool),
	encoder func(O2) O1,
	log logging.Logger) ManagedProposal[I2, O2] {
	return newTranscodingCall[ProposalID, I1, O1, I2, O2](parent, input, decoder, encoder, log)
}

func newTranscodingQuery[I1, O1, I2, O2 any](
	parent ManagedQuery[I1, O1],
	input I2,
	decoder func(I1) (I2, bool),
	encoder func(O2) O1,
	log logging.Logger) ManagedQuery[I2, O2] {
	return newTranscodingCall[QueryID, I1, O1, I2, O2](parent, input, decoder, encoder, log)
}
