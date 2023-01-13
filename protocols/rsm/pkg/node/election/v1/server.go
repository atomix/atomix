// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/election/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		electionprotocolv1.RegisterLeaderElectionServer(server, NewLeaderElectionServer(node))
	})
}

var serverCodec = node.NewCodec[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput](
	func(input *electionprotocolv1.LeaderElectionInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*electionprotocolv1.LeaderElectionOutput, error) {
		output := &electionprotocolv1.LeaderElectionOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewLeaderElectionServer(protocol node.Protocol) electionprotocolv1.LeaderElectionServer {
	return &electionServer{
		handler: node.NewHandler[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput](protocol, serverCodec),
	}
}

type electionServer struct {
	handler node.Handler[*electionprotocolv1.LeaderElectionInput, *electionprotocolv1.LeaderElectionOutput]
}

func (s *electionServer) Enter(ctx context.Context, request *electionprotocolv1.EnterRequest) (*electionprotocolv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Trunc128("EnterRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Enter{
			Enter: request.EnterInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Enter",
			logging.Trunc128("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.EnterResponse{
		Headers:     headers,
		EnterOutput: output.GetEnter(),
	}
	log.Debugw("Enter",
		logging.Trunc128("EnterRequest", request),
		logging.Trunc128("EnterResponse", response))
	return response, nil
}

func (s *electionServer) Withdraw(ctx context.Context, request *electionprotocolv1.WithdrawRequest) (*electionprotocolv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Trunc128("WithdrawRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Withdraw{
			Withdraw: request.WithdrawInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Trunc128("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.WithdrawResponse{
		Headers:        headers,
		WithdrawOutput: output.GetWithdraw(),
	}
	log.Debugw("Withdraw",
		logging.Trunc128("WithdrawRequest", request),
		logging.Trunc128("WithdrawResponse", response))
	return response, nil
}

func (s *electionServer) Anoint(ctx context.Context, request *electionprotocolv1.AnointRequest) (*electionprotocolv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Trunc128("AnointRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Anoint{
			Anoint: request.AnointInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Anoint",
			logging.Trunc128("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.AnointResponse{
		Headers:      headers,
		AnointOutput: output.GetAnoint(),
	}
	log.Debugw("Anoint",
		logging.Trunc128("AnointRequest", request),
		logging.Trunc128("AnointResponse", response))
	return response, nil
}

func (s *electionServer) Promote(ctx context.Context, request *electionprotocolv1.PromoteRequest) (*electionprotocolv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Trunc128("PromoteRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Promote{
			Promote: request.PromoteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Promote",
			logging.Trunc128("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.PromoteResponse{
		Headers:       headers,
		PromoteOutput: output.GetPromote(),
	}
	log.Debugw("Promote",
		logging.Trunc128("PromoteRequest", request),
		logging.Trunc128("PromoteResponse", response))
	return response, nil
}

func (s *electionServer) Demote(ctx context.Context, request *electionprotocolv1.DemoteRequest) (*electionprotocolv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Trunc128("DemoteRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Demote{
			Demote: request.DemoteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Demote",
			logging.Trunc128("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.DemoteResponse{
		Headers:      headers,
		DemoteOutput: output.GetDemote(),
	}
	log.Debugw("Demote",
		logging.Trunc128("DemoteRequest", request),
		logging.Trunc128("DemoteResponse", response))
	return response, nil
}

func (s *electionServer) Evict(ctx context.Context, request *electionprotocolv1.EvictRequest) (*electionprotocolv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Trunc128("EvictRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Evict{
			Evict: request.EvictInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Evict",
			logging.Trunc128("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.EvictResponse{
		Headers:     headers,
		EvictOutput: output.GetEvict(),
	}
	log.Debugw("Evict",
		logging.Trunc128("EvictRequest", request),
		logging.Trunc128("EvictResponse", response))
	return response, nil
}

func (s *electionServer) GetTerm(ctx context.Context, request *electionprotocolv1.GetTermRequest) (*electionprotocolv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Trunc128("GetTermRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_GetTerm{
			GetTerm: request.GetTermInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Trunc128("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.GetTermResponse{
		Headers:       headers,
		GetTermOutput: output.GetGetTerm(),
	}
	log.Debugw("GetTerm",
		logging.Trunc128("GetTermRequest", request),
		logging.Trunc128("GetTermResponse", response))
	return response, nil
}

func (s *electionServer) Watch(request *electionprotocolv1.WatchRequest, server electionprotocolv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Trunc128("WatchRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Watch{
			Watch: request.WatchInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*electionprotocolv1.LeaderElectionOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
			log.Warnw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Error("Error", err))
			stream.Error(err)
			stream.Close()
		}
	}()

	for {
		result, ok := stream.Receive()
		if !ok {
			return nil
		}

		if result.Failed() {
			log.Warnw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Error("Error", result.Error))
			return result.Error
		}

		response := &electionprotocolv1.WatchResponse{
			Headers:     result.Value.Headers,
			WatchOutput: result.Value.Output.GetWatch(),
		}
		log.Debugw("Watch",
			logging.Trunc128("WatchRequest", request),
			logging.Trunc128("WatchResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Trunc128("WatchRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}
