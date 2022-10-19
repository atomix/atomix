// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/protocol"
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
	command := Command[*protocol.CreatePrimitiveResponse](p)
	response, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.CommandRequestHeaders) (*protocol.CreatePrimitiveResponse, error) {
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
	command := Command[*protocol.ClosePrimitiveResponse](p)
	_, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.CommandRequestHeaders) (*protocol.ClosePrimitiveResponse, error) {
		return protocol.NewSessionClient(conn).ClosePrimitive(ctx, &protocol.ClosePrimitiveRequest{
			Headers: headers,
			ClosePrimitiveInput: protocol.ClosePrimitiveInput{
				PrimitiveID: p.id,
			},
		})
	})
	return err
}

type CommandResponse interface {
	GetHeaders() *protocol.CommandResponseHeaders
}

type QueryResponse interface {
	GetHeaders() *protocol.QueryResponseHeaders
}

func Command[T CommandResponse](primitive *PrimitiveClient) *CommandContext[T] {
	headers := &protocol.CommandRequestHeaders{
		OperationRequestHeaders: protocol.OperationRequestHeaders{
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
	return &CommandContext[T]{
		session: primitive.session,
		headers: headers,
	}
}

type CommandContext[T CommandResponse] struct {
	session *SessionClient
	headers *protocol.CommandRequestHeaders
}

func (c *CommandContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.CommandRequestHeaders) (T, error)) (T, error) {
	c.session.recorder.Start(c.headers.SequenceNum)
	defer c.session.recorder.End(c.headers.SequenceNum)
	response, err := f(c.session.conn, c.headers)
	if err != nil {
		return response, err
	}
	headers := response.GetHeaders()
	c.session.lastIndex.Update(headers.Index)
	if headers.Status != protocol.OperationResponseHeaders_OK {
		return response, getErrorFromStatus(headers.Status, headers.Message)
	}
	return response, nil
}

type CommandStream[T CommandResponse] interface {
	Recv() (T, error)
}

func StreamCommand[T CommandResponse](primitive *PrimitiveClient) *StreamCommandContext[T] {
	headers := &protocol.CommandRequestHeaders{
		OperationRequestHeaders: protocol.OperationRequestHeaders{
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
	return &StreamCommandContext[T]{
		session: primitive.session,
		headers: headers,
	}
}

type StreamCommandContext[T CommandResponse] struct {
	session *SessionClient
	headers *protocol.CommandRequestHeaders
}

func (c *StreamCommandContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.CommandRequestHeaders) (CommandStream[T], error)) (CommandStream[T], error) {
	c.session.recorder.Start(c.headers.SequenceNum)
	stream, err := f(c.session.conn, c.headers)
	if err != nil {
		c.session.recorder.End(c.headers.SequenceNum)
		return stream, err
	}
	c.session.recorder.StreamOpen(c.headers)
	return &CommandStreamContext[T]{
		StreamCommandContext: c,
		stream:               stream,
	}, nil
}

type CommandStreamContext[T CommandResponse] struct {
	*StreamCommandContext[T]
	stream                  CommandStream[T]
	lastResponseSequenceNum protocol.SequenceNum
}

func (s *CommandStreamContext[T]) Recv() (T, error) {
	for {
		response, err := s.stream.Recv()
		if err != nil {
			s.session.recorder.StreamClose(s.headers)
			s.session.recorder.End(s.headers.SequenceNum)
			return response, err
		}
		headers := response.GetHeaders()
		s.session.lastIndex.Update(headers.Index)
		if headers.OutputSequenceNum == s.lastResponseSequenceNum+1 {
			s.lastResponseSequenceNum++
			s.session.recorder.StreamReceive(s.headers, headers)
			if headers.Status != protocol.OperationResponseHeaders_OK {
				return response, getErrorFromStatus(headers.Status, headers.Message)
			}
			return response, nil
		}
	}
}

func Query[T QueryResponse](primitive *PrimitiveClient) *QueryContext[T] {
	headers := &protocol.QueryRequestHeaders{
		OperationRequestHeaders: protocol.OperationRequestHeaders{
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

func (c *QueryContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (T, error)) (T, error) {
	c.session.recorder.Start(c.headers.SequenceNum)
	defer c.session.recorder.End(c.headers.SequenceNum)
	response, err := f(c.session.conn, c.headers)
	if err != nil {
		return response, err
	}
	headers := response.GetHeaders()
	c.session.lastIndex.Update(headers.Index)
	if headers.Status != protocol.OperationResponseHeaders_OK {
		return response, getErrorFromStatus(headers.Status, headers.Message)
	}
	return response, nil
}

type QueryStream[T QueryResponse] interface {
	Recv() (T, error)
}

func StreamQuery[T QueryResponse](primitive *PrimitiveClient) *StreamQueryContext[T] {
	headers := &protocol.QueryRequestHeaders{
		OperationRequestHeaders: protocol.OperationRequestHeaders{
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

func (c *StreamQueryContext[T]) Run(f func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (QueryStream[T], error)) (QueryStream[T], error) {
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

func (c *QueryStreamContext[T]) Recv() (T, error) {
	for {
		response, err := c.stream.Recv()
		if err != nil {
			c.session.recorder.End(c.headers.SequenceNum)
			return response, err
		}
		headers := response.GetHeaders()
		c.session.lastIndex.Update(headers.Index)
		if headers.Status != protocol.OperationResponseHeaders_OK {
			return response, getErrorFromStatus(headers.Status, headers.Message)
		}
		return response, nil
	}
}
