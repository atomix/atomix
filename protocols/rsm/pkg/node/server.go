// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
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
		logging.Trunc128("OpenSessionRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("OpenSession",
			logging.Trunc128("OpenSessionRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("OpenSessionRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &protocol.OpenSessionResponse{
		Headers: &protocol.PartitionResponseHeaders{
			Index: output.Index,
		},
		OpenSessionOutput: output.GetOpenSession(),
	}
	log.Debugw("OpenSession",
		logging.Trunc128("OpenSessionRequest", request),
		logging.Trunc128("OpenSessionResponse", response))
	return response, nil
}

func (s *nodeServer) KeepAlive(ctx context.Context, request *protocol.KeepAliveRequest) (*protocol.KeepAliveResponse, error) {
	log.Debugw("KeepAlive",
		logging.Trunc128("KeepAliveRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("KeepAlive",
			logging.Trunc128("KeepAliveRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("KeepAliveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &protocol.KeepAliveResponse{
		Headers: &protocol.PartitionResponseHeaders{
			Index: output.Index,
		},
		KeepAliveOutput: output.GetKeepAlive(),
	}
	log.Debugw("KeepAlive",
		logging.Trunc128("KeepAliveRequest", request),
		logging.Trunc128("KeepAliveResponse", response))
	return response, nil
}

func (s *nodeServer) CloseSession(ctx context.Context, request *protocol.CloseSessionRequest) (*protocol.CloseSessionResponse, error) {
	log.Debugw("CloseSession",
		logging.Trunc128("CloseSessionRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("CloseSession",
			logging.Trunc128("CloseSessionRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("CloseSessionRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &protocol.CloseSessionResponse{
		Headers: &protocol.PartitionResponseHeaders{
			Index: output.Index,
		},
		CloseSessionOutput: output.GetCloseSession(),
	}
	log.Debugw("CloseSession",
		logging.Trunc128("CloseSessionRequest", request),
		logging.Trunc128("CloseSessionResponse", response))
	return response, nil
}

func (s *nodeServer) CreatePrimitive(ctx context.Context, request *protocol.CreatePrimitiveRequest) (*protocol.CreatePrimitiveResponse, error) {
	log.Debugw("CreatePrimitive",
		logging.Trunc128("CreatePrimitiveRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("CreatePrimitive",
			logging.Trunc128("CreatePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("CreatePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, err
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
		logging.Trunc128("CreatePrimitiveRequest", request),
		logging.Trunc128("CreatePrimitiveResponse", response))
	return response, nil
}

func (s *nodeServer) ClosePrimitive(ctx context.Context, request *protocol.ClosePrimitiveRequest) (*protocol.ClosePrimitiveResponse, error) {
	log.Debugw("ClosePrimitive",
		logging.Trunc128("ClosePrimitiveRequest", request))

	partition, ok := s.Partition(request.Headers.PartitionID)
	if !ok {
		err := errors.NewUnavailable("unknown partition %d", request.Headers.PartitionID)
		log.Warnw("ClosePrimitive",
			logging.Trunc128("ClosePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, err
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
			logging.Trunc128("ClosePrimitiveRequest", request),
			logging.Error("Error", err))
		return nil, err
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
		logging.Trunc128("ClosePrimitiveRequest", request),
		logging.Trunc128("ClosePrimitiveResponse", response))
	return response, nil
}
