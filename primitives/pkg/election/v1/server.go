// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	streams "github.com/atomix/runtime/sdk/pkg/stream"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const truncLen = 200

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		RegisterLeaderElectionServer(server, NewLeaderElectionServer(node))
	})
}

var serverCodec = node.NewCodec[*LeaderElectionInput, *LeaderElectionOutput](
	func(input *LeaderElectionInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*LeaderElectionOutput, error) {
		output := &LeaderElectionOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewLeaderElectionServer(protocol node.Protocol) LeaderElectionServer {
	return &electionServer{
		handler: node.NewHandler[*LeaderElectionInput, *LeaderElectionOutput](protocol, serverCodec),
	}
}

type electionServer struct {
	handler node.Handler[*LeaderElectionInput, *LeaderElectionOutput]
}

func (s *electionServer) Enter(ctx context.Context, request *EnterRequest) (*EnterResponse, error) {
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Enter{
			Enter: request.EnterInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &EnterResponse{
		Headers:     headers,
		EnterOutput: output.GetEnter(),
	}
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("EnterResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) Withdraw(ctx context.Context, request *WithdrawRequest) (*WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Withdraw{
			Withdraw: request.WithdrawInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &WithdrawResponse{
		Headers:        headers,
		WithdrawOutput: output.GetWithdraw(),
	}
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("WithdrawResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) Anoint(ctx context.Context, request *AnointRequest) (*AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Anoint{
			Anoint: request.AnointInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &AnointResponse{
		Headers:      headers,
		AnointOutput: output.GetAnoint(),
	}
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AnointResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) Promote(ctx context.Context, request *PromoteRequest) (*PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Promote{
			Promote: request.PromoteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &PromoteResponse{
		Headers:       headers,
		PromoteOutput: output.GetPromote(),
	}
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PromoteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) Demote(ctx context.Context, request *DemoteRequest) (*DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Demote{
			Demote: request.DemoteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &DemoteResponse{
		Headers:      headers,
		DemoteOutput: output.GetDemote(),
	}
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("DemoteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) Evict(ctx context.Context, request *EvictRequest) (*EvictResponse, error) {
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Evict{
			Evict: request.EvictInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &EvictResponse{
		Headers:     headers,
		EvictOutput: output.GetEvict(),
	}
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("EvictResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) GetTerm(ctx context.Context, request *GetTermRequest) (*GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_GetTerm{
			GetTerm: request.GetTermInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &GetTermResponse{
		Headers:       headers,
		GetTermOutput: output.GetGetTerm(),
	}
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetTermResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *electionServer) Watch(request *WatchRequest, server LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)))
	input := &LeaderElectionInput{
		Input: &LeaderElectionInput_Watch{
			Watch: request.WatchInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*LeaderElectionOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
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
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}

		response := &WatchResponse{
			Headers:     result.Value.Headers,
			WatchOutput: result.Value.Output.GetWatch(),
		}
		log.Debugw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("WatchResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}
