// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/election/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const truncLen = 200

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
		logging.Stringer("EnterRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Enter{
			Enter: request.EnterInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.EnterResponse{
		Headers:     headers,
		EnterOutput: output.GetEnter(),
	}
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", request),
		logging.Stringer("EnterResponse", response))
	return response, nil
}

func (s *electionServer) Withdraw(ctx context.Context, request *electionprotocolv1.WithdrawRequest) (*electionprotocolv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Withdraw{
			Withdraw: request.WithdrawInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.WithdrawResponse{
		Headers:        headers,
		WithdrawOutput: output.GetWithdraw(),
	}
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", request),
		logging.Stringer("WithdrawResponse", response))
	return response, nil
}

func (s *electionServer) Anoint(ctx context.Context, request *electionprotocolv1.AnointRequest) (*electionprotocolv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Anoint{
			Anoint: request.AnointInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.AnointResponse{
		Headers:      headers,
		AnointOutput: output.GetAnoint(),
	}
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", request),
		logging.Stringer("AnointResponse", response))
	return response, nil
}

func (s *electionServer) Promote(ctx context.Context, request *electionprotocolv1.PromoteRequest) (*electionprotocolv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Promote{
			Promote: request.PromoteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.PromoteResponse{
		Headers:       headers,
		PromoteOutput: output.GetPromote(),
	}
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", request),
		logging.Stringer("PromoteResponse", response))
	return response, nil
}

func (s *electionServer) Demote(ctx context.Context, request *electionprotocolv1.DemoteRequest) (*electionprotocolv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Demote{
			Demote: request.DemoteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.DemoteResponse{
		Headers:      headers,
		DemoteOutput: output.GetDemote(),
	}
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", request),
		logging.Stringer("DemoteResponse", response))
	return response, nil
}

func (s *electionServer) Evict(ctx context.Context, request *electionprotocolv1.EvictRequest) (*electionprotocolv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Evict{
			Evict: request.EvictInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.EvictResponse{
		Headers:     headers,
		EvictOutput: output.GetEvict(),
	}
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", request),
		logging.Stringer("EvictResponse", response))
	return response, nil
}

func (s *electionServer) GetTerm(ctx context.Context, request *electionprotocolv1.GetTermRequest) (*electionprotocolv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_GetTerm{
			GetTerm: request.GetTermInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionprotocolv1.GetTermResponse{
		Headers:       headers,
		GetTermOutput: output.GetGetTerm(),
	}
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", request),
		logging.Stringer("GetTermResponse", response))
	return response, nil
}

func (s *electionServer) Watch(request *electionprotocolv1.WatchRequest, server electionprotocolv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", request))
	input := &electionprotocolv1.LeaderElectionInput{
		Input: &electionprotocolv1.LeaderElectionInput_Watch{
			Watch: request.WatchInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*electionprotocolv1.LeaderElectionOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", request),
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
			err := errors.ToProto(result.Error)
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", request),
				logging.Error("Error", err))
			return err
		}

		response := &electionprotocolv1.WatchResponse{
			Headers:     result.Value.Headers,
			WatchOutput: result.Value.Output.GetWatch(),
		}
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", request),
			logging.Stringer("WatchResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}
