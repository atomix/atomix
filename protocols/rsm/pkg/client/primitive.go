// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"google.golang.org/grpc"
)

func newPrimitiveClient(session *SessionClient, spec protocol.PrimitiveSpec) *PrimitiveClient {
	return &PrimitiveClient{
		session: session,
		spec:    spec,
	}
}

type PrimitiveClient struct {
	session *SessionClient
	id      protocol.PrimitiveID
	spec    protocol.PrimitiveSpec
}

func (p *PrimitiveClient) open(ctx context.Context) error {
	command := Proposal[*protocol.CreatePrimitiveResponse](p)
	response, _, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*protocol.CreatePrimitiveResponse, error) {
		return protocol.NewSessionClient(conn).CreatePrimitive(ctx, &protocol.CreatePrimitiveRequest{
			Headers: headers,
			CreatePrimitiveInput: protocol.CreatePrimitiveInput{
				PrimitiveSpec: p.spec,
			},
		})
	})
	if err != nil {
		return err
	}
	p.id = response.PrimitiveID
	return nil
}

func (p *PrimitiveClient) close(ctx context.Context) error {
	command := Proposal[*protocol.ClosePrimitiveResponse](p)
	_, _, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*protocol.ClosePrimitiveResponse, error) {
		return protocol.NewSessionClient(conn).ClosePrimitive(ctx, &protocol.ClosePrimitiveRequest{
			Headers: headers,
			ClosePrimitiveInput: protocol.ClosePrimitiveInput{
				PrimitiveID: p.id,
			},
		})
	})
	return err
}

type ProposalResponse interface {
	GetHeaders() *protocol.ProposalResponseHeaders
}

type QueryResponse interface {
	GetHeaders() *protocol.QueryResponseHeaders
}

func Proposal[T ProposalResponse](primitive *PrimitiveClient) *ProposalContext[T] {
	headers := &protocol.ProposalRequestHeaders{
		CallRequestHeaders: protocol.CallRequestHeaders{
			PrimitiveRequestHeaders: protocol.PrimitiveRequestHeaders{
				SessionRequestHeaders: protocol.SessionRequestHeaders{
					PartitionRequestHeaders: protocol.PartitionRequestHeaders{
						PartitionID: primitive.session.partition.id,
					},
					SessionID: primitive.session.sessionID,
				},
				PrimitiveID: primitive.id,
			},
		},
		SequenceNum: primitive.session.nextRequestNum(),
	}
	return &ProposalContext[T]{
		session: primitive.session,
		Headers: headers,
	}
}

type ProposalContext[T ProposalResponse] struct {
	session *SessionClient
	Headers *protocol.ProposalRequestHeaders
}

func (c *ProposalContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (T, error)) (T, bool, error) {
	c.session.recorder.Start(c.Headers.SequenceNum)
	defer c.session.recorder.End(c.Headers.SequenceNum)
	response, err := f(c.session.conn, c.Headers)
	if err != nil {
		return response, false, err
	}
	headers := response.GetHeaders()
	c.session.lastIndex.Update(headers.Index)
	if headers.Status != protocol.CallResponseHeaders_OK {
		return response, true, getErrorFromStatus(headers.Status, headers.Message)
	}
	return response, true, nil
}

type ProposalStream[T ProposalResponse] interface {
	Recv() (T, error)
}

func StreamProposal[T ProposalResponse](primitive *PrimitiveClient) *StreamProposalContext[T] {
	headers := &protocol.ProposalRequestHeaders{
		CallRequestHeaders: protocol.CallRequestHeaders{
			PrimitiveRequestHeaders: protocol.PrimitiveRequestHeaders{
				SessionRequestHeaders: protocol.SessionRequestHeaders{
					PartitionRequestHeaders: protocol.PartitionRequestHeaders{
						PartitionID: primitive.session.partition.id,
					},
					SessionID: primitive.session.sessionID,
				},
				PrimitiveID: primitive.id,
			},
		},
		SequenceNum: primitive.session.nextRequestNum(),
	}
	return &StreamProposalContext[T]{
		session: primitive.session,
		headers: headers,
	}
}

type StreamProposalContext[T ProposalResponse] struct {
	session *SessionClient
	headers *protocol.ProposalRequestHeaders
}

func (c *StreamProposalContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (ProposalStream[T], error)) (*ProposalStreamContext[T], error) {
	c.session.recorder.Start(c.headers.SequenceNum)
	stream, err := f(c.session.conn, c.headers)
	if err != nil {
		c.session.recorder.End(c.headers.SequenceNum)
		return nil, err
	}
	c.session.recorder.StreamOpen(c.headers)
	return &ProposalStreamContext[T]{
		StreamProposalContext: c,
		stream:                stream,
	}, nil
}

type ProposalStreamContext[T ProposalResponse] struct {
	*StreamProposalContext[T]
	stream                  ProposalStream[T]
	lastResponseSequenceNum protocol.SequenceNum
}

func (s *ProposalStreamContext[T]) Recv() (T, bool, error) {
	for {
		response, err := s.stream.Recv()
		if err != nil {
			s.session.recorder.StreamClose(s.headers)
			s.session.recorder.End(s.headers.SequenceNum)
			return response, false, err
		}
		headers := response.GetHeaders()
		s.session.lastIndex.Update(headers.Index)
		if headers.OutputSequenceNum == s.lastResponseSequenceNum+1 {
			s.lastResponseSequenceNum++
			s.session.recorder.StreamReceive(s.headers, headers)
			if headers.Status != protocol.CallResponseHeaders_OK {
				return response, true, getErrorFromStatus(headers.Status, headers.Message)
			}
			return response, true, nil
		}
	}
}

func Query[T QueryResponse](primitive *PrimitiveClient) *QueryContext[T] {
	headers := &protocol.QueryRequestHeaders{
		CallRequestHeaders: protocol.CallRequestHeaders{
			PrimitiveRequestHeaders: protocol.PrimitiveRequestHeaders{
				SessionRequestHeaders: protocol.SessionRequestHeaders{
					PartitionRequestHeaders: protocol.PartitionRequestHeaders{
						PartitionID: primitive.session.partition.id,
					},
					SessionID: primitive.session.sessionID,
				},
				PrimitiveID: primitive.id,
			},
		},
		SequenceNum:      primitive.session.nextRequestNum(),
		MaxReceivedIndex: primitive.session.lastIndex.Get(),
	}
	return &QueryContext[T]{
		session: primitive.session,
		headers: headers,
	}
}

type QueryContext[T QueryResponse] struct {
	session *SessionClient
	headers *protocol.QueryRequestHeaders
}

func (c *QueryContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (T, error)) (T, bool, error) {
	c.session.recorder.Start(c.headers.SequenceNum)
	defer c.session.recorder.End(c.headers.SequenceNum)
	response, err := f(c.session.conn, c.headers)
	if err != nil {
		return response, false, err
	}
	headers := response.GetHeaders()
	c.session.lastIndex.Update(headers.Index)
	if headers.Status != protocol.CallResponseHeaders_OK {
		return response, true, getErrorFromStatus(headers.Status, headers.Message)
	}
	return response, true, nil
}

type QueryStream[T QueryResponse] interface {
	Recv() (T, error)
}

func StreamQuery[T QueryResponse](primitive *PrimitiveClient) *StreamQueryContext[T] {
	headers := &protocol.QueryRequestHeaders{
		CallRequestHeaders: protocol.CallRequestHeaders{
			PrimitiveRequestHeaders: protocol.PrimitiveRequestHeaders{
				SessionRequestHeaders: protocol.SessionRequestHeaders{
					PartitionRequestHeaders: protocol.PartitionRequestHeaders{
						PartitionID: primitive.session.partition.id,
					},
					SessionID: primitive.session.sessionID,
				},
				PrimitiveID: primitive.id,
			},
		},
		SequenceNum:      primitive.session.nextRequestNum(),
		MaxReceivedIndex: primitive.session.lastIndex.Get(),
	}
	return &StreamQueryContext[T]{
		session: primitive.session,
		headers: headers,
	}
}

type StreamQueryContext[T QueryResponse] struct {
	session *SessionClient
	headers *protocol.QueryRequestHeaders
}

func (c *StreamQueryContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (QueryStream[T], error)) (*QueryStreamContext[T], error) {
	c.session.recorder.Start(c.headers.SequenceNum)
	stream, err := f(c.session.conn, c.headers)
	if err != nil {
		c.session.recorder.End(c.headers.SequenceNum)
		return nil, err
	}
	return &QueryStreamContext[T]{
		StreamQueryContext: c,
		stream:             stream,
	}, nil
}

type QueryStreamContext[T QueryResponse] struct {
	*StreamQueryContext[T]
	stream QueryStream[T]
}

func (c *QueryStreamContext[T]) Recv() (T, bool, error) {
	response, err := c.stream.Recv()
	if err != nil {
		c.session.recorder.End(c.headers.SequenceNum)
		return response, false, err
	}
	headers := response.GetHeaders()
	c.session.lastIndex.Update(headers.Index)
	if headers.Status != protocol.CallResponseHeaders_OK {
		return response, true, getErrorFromStatus(headers.Status, headers.Message)
	}
	return response, true, nil
}
