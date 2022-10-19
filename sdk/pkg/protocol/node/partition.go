// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	streams "github.com/atomix/runtime/sdk/pkg/stream"
)

func NewPartition(id protocol.PartitionID, executor Executor) *Partition {
	return &Partition{
		id:       id,
		executor: executor,
	}
}

type Partition struct {
	id       protocol.PartitionID
	executor Executor
}

func (p *Partition) ID() protocol.PartitionID {
	return p.id
}

func (p *Partition) Command(ctx context.Context, command *protocol.ProposalInput) (*protocol.ProposalOutput, error) {
	resultCh := make(chan streams.Result[*protocol.ProposalOutput], 1)
	errCh := make(chan error, 1)
	go func() {
		if err := p.executor.Propose(ctx, command, streams.NewChannelStream[*protocol.ProposalOutput](resultCh)); err != nil {
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

func (p *Partition) StreamCommand(ctx context.Context, input *protocol.ProposalInput, stream streams.WriteStream[*protocol.ProposalOutput]) error {
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

func (p *Partition) Query(ctx context.Context, input *protocol.QueryInput) (*protocol.QueryOutput, error) {
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

func (p *Partition) StreamQuery(ctx context.Context, input *protocol.QueryInput, stream streams.WriteStream[*protocol.QueryOutput]) error {
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
