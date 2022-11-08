// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"time"
)

func newServer(protocol Protocol) *nodeServer {
	return &nodeServer{
		Protocol: protocol,
	}
}

type nodeServer struct {
	Protocol
}

func (s *nodeServer) OpenSession(ctx context.Context, request *protocol.OpenSessionRequest) (*protocol.OpenSessionResponse, error) {
	log.Debugw("OpenSession",
		logging.Stringer("OpenSessionRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("OpenSession",
			logging.Stringer("OpenSessionRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	input := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_OpenSession{
			OpenSession: request.OpenSessionInput,
		},
	}
	output, err := partition.Propose(ctx, input)
	if err != nil {
		log.Warnw("OpenSession",
			logging.Stringer("OpenSessionRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &protocol.OpenSessionResponse{
		Headers: &protocol.PartitionResponseHeaders{
			Index: output.Index,
		},
		OpenSessionOutput: output.GetOpenSession(),
	}
	log.Debugw("OpenSession",
		logging.Stringer("OpenSessionRequest", request),
		logging.Stringer("OpenSessionResponse", response))
	return response, nil
}

func (s *nodeServer) KeepAlive(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
	log.Debugw("KeepAlive",
		logging.Stringer("KeepAliveRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("KeepAlive",
			logging.Stringer("KeepAliveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	command := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_KeepAlive{
			KeepAlive: request.KeepAliveInput,
		},
	}
	output, err := partition.Propose(ctx, command)
	if err != nil {
		log.Warnw("KeepAlive",
			logging.Stringer("KeepAliveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &protocol.KeepAliveResponse{
		Headers: &protocol.PartitionResponseHeaders{
			Index: output.Index,
		},
		KeepAliveOutput: output.GetKeepAlive(),
	}
	log.Debugw("KeepAlive",
		logging.Stringer("KeepAliveRequest", request),
		logging.Stringer("KeepAliveResponse", response))
	return response, nil
}

func (s *nodeServer) CloseSession(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
	log.Debugw("CloseSession",
		logging.Stringer("CloseSessionRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("CloseSession",
			logging.Stringer("CloseSessionRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	command := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_CloseSession{
			CloseSession: request.CloseSessionInput,
		},
	}
	output, err := partition.Propose(ctx, command)
	if err != nil {
		log.Warnw("CloseSession",
			logging.Stringer("CloseSessionRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &protocol.CloseSessionResponse{
		Headers: &protocol.PartitionResponseHeaders{
			Index: output.Index,
		},
		CloseSessionOutput: output.GetCloseSession(),
	}
	log.Debugw("CloseSession",
		logging.Stringer("CloseSessionRequest", request),
		logging.Stringer("CloseSessionResponse", response))
	return response, nil
}

func (s *nodeServer) CreatePrimitive(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
	log.Debugw("CreatePrimitive",
		logging.Stringer("CreatePrimitiveRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("CreatePrimitive",
			logging.Stringer("CreatePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	command := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_Proposal{
			Proposal: &protocol.SessionProposalInput{
				SessionID:   request.Headers.SessionID,
				SequenceNum: request.Headers.SequenceNum,
				Input: &protocol.SessionProposalInput_CreatePrimitive{
					CreatePrimitive: &request.CreatePrimitiveInput,
				},
			},
		},
	}
	output, err := partition.Propose(ctx, command)
	if err != nil {
		log.Warnw("CreatePrimitive",
			logging.Stringer("CreatePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &protocol.CreatePrimitiveResponse{
		Headers: &protocol.ProposalResponseHeaders{
			CallResponseHeaders: protocol.CallResponseHeaders{
				PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
					SessionResponseHeaders: protocol.SessionResponseHeaders{
						PartitionResponseHeaders: protocol.PartitionResponseHeaders{
							Index: output.Index,
						},
					},
				},
				Status:  getHeaderStatus(output.GetProposal().Failure),
				Message: getHeaderMessage(output.GetProposal().Failure),
			},
		},
		CreatePrimitiveOutput: output.GetProposal().GetCreatePrimitive(),
	}
	log.Debugw("CreatePrimitive",
		logging.Stringer("CreatePrimitiveRequest", request),
		logging.Stringer("CreatePrimitiveResponse", response))
	return response, nil
}

func (s *nodeServer) ClosePrimitive(ctx context.Context, request *protocol.ClosePrimitiveRequest) (*protocol.ClosePrimitiveResponse, error) {
	log.Debugw("ClosePrimitive",
		logging.Stringer("ClosePrimitiveRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("ClosePrimitive",
			logging.Stringer("ClosePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	command := &protocol.ProposalInput{
		Timestamp: time.Now(),
		Input: &protocol.ProposalInput_Proposal{
			Proposal: &protocol.SessionProposalInput{
				SessionID:   request.Headers.SessionID,
				SequenceNum: request.Headers.SequenceNum,
				Input: &protocol.SessionProposalInput_ClosePrimitive{
					ClosePrimitive: &request.ClosePrimitiveInput,
				},
			},
		},
	}
	output, err := partition.Propose(ctx, command)
	if err != nil {
		log.Warnw("ClosePrimitive",
			logging.Stringer("ClosePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &protocol.ClosePrimitiveResponse{
		Headers: &protocol.ProposalResponseHeaders{
			CallResponseHeaders: protocol.CallResponseHeaders{
				PrimitiveResponseHeaders: protocol.PrimitiveResponseHeaders{
					SessionResponseHeaders: protocol.SessionResponseHeaders{
						PartitionResponseHeaders: protocol.PartitionResponseHeaders{
							Index: output.Index,
						},
					},
				},
				Status:  getHeaderStatus(output.GetProposal().Failure),
				Message: getHeaderMessage(output.GetProposal().Failure),
			},
		},
		ClosePrimitiveOutput: output.GetProposal().GetClosePrimitive(),
	}
	log.Debugw("ClosePrimitive",
		logging.Stringer("ClosePrimitiveRequest", request),
		logging.Stringer("ClosePrimitiveResponse", response))
	return response, nil
}
