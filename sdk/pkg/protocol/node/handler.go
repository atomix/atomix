// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	streams "github.com/atomix/runtime/sdk/pkg/stream"
	"github.com/gogo/protobuf/proto"
	"time"
)

type Handler[I any, O any] interface {
	Propose(ctx context.Context, input I, headers *protocol.ProposalRequestHeaders) (O, *protocol.ProposalResponseHeaders, error)
	StreamPropose(ctx context.Context, input I, headers *protocol.ProposalRequestHeaders, stream streams.WriteStream[*StreamProposalResponse[O]]) error
	Query(ctx context.Context, input I, headers *protocol.QueryRequestHeaders) (O, *protocol.QueryResponseHeaders, error)
	StreamQuery(ctx context.Context, input I, headers *protocol.QueryRequestHeaders, stream streams.WriteStream[*StreamQueryResponse[O]]) error
}

type StreamResponse[O any, H proto.Message] struct {
	Headers H
	Output  O
}

type StreamProposalResponse[O any] StreamResponse[O, *protocol.ProposalResponseHeaders]

type StreamQueryResponse[O any] StreamResponse[O, *protocol.QueryResponseHeaders]

func NewHandler[I, O proto.Message](protocol Protocol, codec Codec[I, O]) Handler[I, O] {
	return &nodeHandler[I, O]{
		protocol: protocol,
		codec:    codec,
	}
}

type nodeHandler[I, O proto.Message] struct {
	protocol Protocol
	codec    Codec[I, O]
}

func (p *nodeHandler[I, O]) Propose(ctx context.Context, input I, inputHeaders *protocol.ProposalRequestHeaders) (O, *protocol.ProposalResponseHeaders, error) {
	var output O
	inputBytes, err := p.codec.EncodeInput(input)
	if err != nil {
		return output, nil, errors.NewInternal(err.Error())
	}

	partition, ok := p.protocol.Partition(inputHeaders.PartitionID)
	if !ok {
		return output, nil, errors.NewUnavailable("unknown partition %d", inputHeaders.PartitionID)
	}

	proposalInput := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_Proposal{
			Proposal: &protocol.SessionProposalInput{
				SessionID:   inputHeaders.SessionID,
				SequenceNum: inputHeaders.SequenceNum,
				Input: &protocol.SessionProposalInput_Proposal{
					Proposal: &protocol.PrimitiveProposalInput{
						PrimitiveID: inputHeaders.PrimitiveID,
						Payload:     inputBytes,
					},
				},
			},
		},
	}

	proposalOutput, err := partition.Propose(ctx, proposalInput)
	if err != nil {
		return output, nil, err
	}

	outputHeaders := &protocol.ProposalResponseHeaders{
		CallResponseHeaders: protocol.CallResponseHeaders{
			PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
				SessionResponseHeaders: protocol.SessionResponseHeaders{
					PartitionResponseHeaders: protocol.PartitionResponseHeaders{
						Index: proposalOutput.Index,
					},
				},
			},
			Status:  getHeaderStatus(proposalOutput.GetProposal().Failure),
			Message: getHeaderMessage(proposalOutput.GetProposal().Failure),
		},
		OutputSequenceNum: proposalOutput.GetProposal().SequenceNum,
	}
	if outputHeaders.Status != protocol.CallResponseHeaders_OK {
		return output, outputHeaders, nil
	}

	outputBytes := proposalOutput.GetProposal().GetProposal().Payload
	output, err = p.codec.DecodeOutput(outputBytes)
	if err != nil {
		return output, nil, err
	}
	return output, outputHeaders, nil
}

func (p *nodeHandler[I, O]) StreamPropose(ctx context.Context, input I, inputHeaders *protocol.ProposalRequestHeaders, stream streams.WriteStream[*StreamProposalResponse[O]]) error {
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		return errors.NewInternal(err.Error())
	}

	partition, ok := p.protocol.Partition(inputHeaders.PartitionID)
	if !ok {
		return errors.NewUnavailable("unknown partition %d", inputHeaders.PartitionID)
	}

	proposalInput := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_Proposal{
			Proposal: &protocol.SessionProposalInput{
				SessionID:   inputHeaders.SessionID,
				SequenceNum: inputHeaders.SequenceNum,
				Input: &protocol.SessionProposalInput_Proposal{
					Proposal: &protocol.PrimitiveProposalInput{
						PrimitiveID: inputHeaders.PrimitiveID,
						Payload:     inputBytes,
					},
				},
			},
		},
	}

	return partition.StreamPropose(ctx, proposalInput, streams.NewEncodingStream[*protocol.ProposalOutput, *StreamProposalResponse[O]](stream, func(proposalOutput *protocol.ProposalOutput, err error) (*StreamProposalResponse[O], error) {
		if err != nil {
			return nil, err
		}
		outputHeaders := &protocol.ProposalResponseHeaders{
			CallResponseHeaders: protocol.CallResponseHeaders{
				PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
					SessionResponseHeaders: protocol.SessionResponseHeaders{
						PartitionResponseHeaders: protocol.PartitionResponseHeaders{
							Index: proposalOutput.Index,
						},
					},
				},
				Status:  getHeaderStatus(proposalOutput.GetProposal().Failure),
				Message: getHeaderMessage(proposalOutput.GetProposal().Failure),
			},
			OutputSequenceNum: proposalOutput.GetProposal().SequenceNum,
		}
		var payload []byte
		if outputHeaders.Status == protocol.CallResponseHeaders_OK {
			payload = proposalOutput.GetProposal().GetProposal().Payload
		}
		output, err := p.codec.DecodeOutput(payload)
		if err != nil {
			return nil, err
		}
		return &StreamProposalResponse[O]{
			Headers: outputHeaders,
			Output:  output,
		}, nil
	}))
}

func (p *nodeHandler[I, O]) Query(ctx context.Context, input I, inputHeaders *protocol.QueryRequestHeaders) (O, *protocol.QueryResponseHeaders, error) {
	var output O
	inputBytes, err := p.codec.EncodeInput(input)
	if err != nil {
		return output, nil, errors.NewInternal(err.Error())
	}

	partition, ok := p.protocol.Partition(inputHeaders.PartitionID)
	if !ok {
		return output, nil, errors.NewUnavailable("unknown partition %d", inputHeaders.PartitionID)
	}

	queryInput := &protocol.QueryInput{
		MaxReceivedIndex: inputHeaders.MaxReceivedIndex,
		Input: &protocol.QueryInput_Query{
			Query: &protocol.SessionQueryInput{
				SessionID:   inputHeaders.SessionID,
				SequenceNum: inputHeaders.SequenceNum,
				Input: &protocol.SessionQueryInput_Query{
					Query: &protocol.PrimitiveQueryInput{
						PrimitiveID: inputHeaders.PrimitiveID,
						Payload:     inputBytes,
					},
				},
			},
		},
	}

	queryOutput, err := partition.Query(ctx, queryInput)
	if err != nil {
		return output, nil, err
	}

	outputHeaders := &protocol.QueryResponseHeaders{
		CallResponseHeaders: protocol.CallResponseHeaders{
			PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
				SessionResponseHeaders: protocol.SessionResponseHeaders{
					PartitionResponseHeaders: protocol.PartitionResponseHeaders{
						Index: queryOutput.Index,
					},
				},
			},
			Status:  getHeaderStatus(queryOutput.GetQuery().Failure),
			Message: getHeaderMessage(queryOutput.GetQuery().Failure),
		},
	}
	if outputHeaders.Status != protocol.CallResponseHeaders_OK {
		return output, outputHeaders, nil
	}

	outputBytes := queryOutput.GetQuery().GetQuery().Payload
	output, err = p.codec.DecodeOutput(outputBytes)
	if err != nil {
		return output, nil, err
	}
	return output, outputHeaders, nil
}

func (p *nodeHandler[I, O]) StreamQuery(ctx context.Context, input I, inputHeaders *protocol.QueryRequestHeaders, stream streams.WriteStream[*StreamQueryResponse[O]]) error {
	inputBytes, err := proto.Marshal(input)
	if err != nil {
		return errors.NewInternal(err.Error())
	}

	partition, ok := p.protocol.Partition(inputHeaders.PartitionID)
	if !ok {
		return errors.NewUnavailable("unknown partition %d", inputHeaders.PartitionID)
	}

	queryInput := &protocol.QueryInput{
		MaxReceivedIndex: inputHeaders.MaxReceivedIndex,
		Input: &protocol.QueryInput_Query{
			Query: &protocol.SessionQueryInput{
				SessionID:   inputHeaders.SessionID,
				SequenceNum: inputHeaders.SequenceNum,
				Input: &protocol.SessionQueryInput_Query{
					Query: &protocol.PrimitiveQueryInput{
						PrimitiveID: inputHeaders.PrimitiveID,
						Payload:     inputBytes,
					},
				},
			},
		},
	}

	return partition.StreamQuery(ctx, queryInput, streams.NewEncodingStream[*protocol.QueryOutput, *StreamQueryResponse[O]](stream, func(queryOutput *protocol.QueryOutput, err error) (*StreamQueryResponse[O], error) {
		if err != nil {
			return nil, err
		}
		outputHeaders := &protocol.QueryResponseHeaders{
			CallResponseHeaders: protocol.CallResponseHeaders{
				PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
					SessionResponseHeaders: protocol.SessionResponseHeaders{
						PartitionResponseHeaders: protocol.PartitionResponseHeaders{
							Index: queryOutput.Index,
						},
					},
				},
				Status:  getHeaderStatus(queryOutput.GetQuery().Failure),
				Message: getHeaderMessage(queryOutput.GetQuery().Failure),
			},
		}
		var payload []byte
		if outputHeaders.Status == protocol.CallResponseHeaders_OK {
			payload = queryOutput.GetQuery().GetQuery().Payload
		}
		output, err := p.codec.DecodeOutput(payload)
		if err != nil {
			return nil, err
		}
		return &StreamQueryResponse[O]{
			Headers: outputHeaders,
			Output:  output,
		}, nil
	}))
}

func getHeaderStatus(failure *protocol.Failure) protocol.CallResponseHeaders_Status {
	if failure == nil {
		return protocol.CallResponseHeaders_OK
	}
	switch failure.Status {
	case protocol.Failure_UNKNOWN:
		return protocol.CallResponseHeaders_UNKNOWN
	case protocol.Failure_ERROR:
		return protocol.CallResponseHeaders_ERROR
	case protocol.Failure_CANCELED:
		return protocol.CallResponseHeaders_CANCELED
	case protocol.Failure_NOT_FOUND:
		return protocol.CallResponseHeaders_NOT_FOUND
	case protocol.Failure_ALREADY_EXISTS:
		return protocol.CallResponseHeaders_ALREADY_EXISTS
	case protocol.Failure_UNAUTHORIZED:
		return protocol.CallResponseHeaders_UNAUTHORIZED
	case protocol.Failure_FORBIDDEN:
		return protocol.CallResponseHeaders_FORBIDDEN
	case protocol.Failure_CONFLICT:
		return protocol.CallResponseHeaders_CONFLICT
	case protocol.Failure_INVALID:
		return protocol.CallResponseHeaders_INVALID
	case protocol.Failure_UNAVAILABLE:
		return protocol.CallResponseHeaders_UNAVAILABLE
	case protocol.Failure_NOT_SUPPORTED:
		return protocol.CallResponseHeaders_NOT_SUPPORTED
	case protocol.Failure_TIMEOUT:
		return protocol.CallResponseHeaders_TIMEOUT
	case protocol.Failure_INTERNAL:
		return protocol.CallResponseHeaders_INTERNAL
	case protocol.Failure_FAULT:
		return protocol.CallResponseHeaders_FAULT
	default:
		return protocol.CallResponseHeaders_UNKNOWN
	}
}

func getHeaderMessage(failure *protocol.Failure) string {
	if failure != nil {
		return failure.Message
	}
	return ""
}
