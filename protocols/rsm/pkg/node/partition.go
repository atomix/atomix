// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
)

type Partition interface {
	ID() protocol.PartitionID
	Propose(ctx context.Context, input *protocol.ProposalInput) (*protocol.ProposalOutput, error)
	StreamPropose(ctx context.Context, input *protocol.ProposalInput, stream streams.WriteStream[*protocol.ProposalOutput]) error
	Query(ctx context.Context, input *protocol.QueryInput) (*protocol.QueryOutput, error)
	StreamQuery(ctx context.Context, input *protocol.QueryInput, stream streams.WriteStream[*protocol.QueryOutput]) error
}

func NewPartition(id protocol.PartitionID, executor Executor) Partition {
	return &nodePartition{
		id:       id,
		executor: executor,
	}
}

type nodePartition struct {
	id       protocol.PartitionID
	executor Executor
}

func (p *nodePartition) ID() protocol.PartitionID {
	return p.id
}

func (p *nodePartition) Propose(ctx context.Context, input *protocol.ProposalInput) (*protocol.ProposalOutput, error) {
	resultCh := make(chan streams.Result[*protocol.ProposalOutput], 1)
	errCh := make(chan error, 1)
	go func() {
		if err := p.executor.Propose(ctx, input, streams.NewChannelStream[*protocol.ProposalOutput](resultCh)); err != nil {
			errCh <- err
		}
	}()

	select {
	case result, ok := <-resultCh:
		if !ok {
			err := errors.NewCanceled("stream closed")
			return nil, err
		}

		if result.Failed() {
			return nil, result.Error
		}

		return result.Value, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *nodePartition) StreamPropose(ctx context.Context, input *protocol.ProposalInput, stream streams.WriteStream[*protocol.ProposalOutput]) error {
	resultCh := make(chan streams.Result[*protocol.ProposalOutput])
	go func() {
		if err := p.executor.Propose(ctx, input, streams.NewChannelStream[*protocol.ProposalOutput](resultCh)); err != nil {
			stream.Error(err)
			stream.Close()
			return
		}
	}()
	go func() {
		defer stream.Close()
		for result := range resultCh {
			stream.Send(result)
		}
	}()
	return nil
}

func (p *nodePartition) Query(ctx context.Context, input *protocol.QueryInput) (*protocol.QueryOutput, error) {
	resultCh := make(chan streams.Result[*protocol.QueryOutput], 1)
	errCh := make(chan error, 1)
	go func() {
		if err := p.executor.Query(ctx, input, streams.NewChannelStream[*protocol.QueryOutput](resultCh)); err != nil {
			errCh <- err
		}
	}()

	select {
	case result, ok := <-resultCh:
		if !ok {
			err := errors.NewCanceled("stream closed")
			return nil, err
		}

		if result.Failed() {
			return nil, result.Error
		}

		return result.Value, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *nodePartition) StreamQuery(ctx context.Context, input *protocol.QueryInput, stream streams.WriteStream[*protocol.QueryOutput]) error {
	resultCh := make(chan streams.Result[*protocol.QueryOutput])
	go func() {
		if err := p.executor.Query(ctx, input, streams.NewChannelStream[*protocol.QueryOutput](resultCh)); err != nil {
			stream.Error(err)
			stream.Close()
			return
		}
	}()
	go func() {
		defer stream.Close()
		for result := range resultCh {
			stream.Send(result)
		}
	}()
	return nil
}
