// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/counter/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const truncLen = 200

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		counterprotocolv1.RegisterCounterServer(server, NewCounterServer(node))
	})
}

var serverCodec = node.NewCodec[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput](
	func(input *counterprotocolv1.CounterInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*counterprotocolv1.CounterOutput, error) {
		output := &counterprotocolv1.CounterOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewCounterServer(protocol node.Protocol) counterprotocolv1.CounterServer {
	return &counterServer{
		handler: node.NewHandler[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput](protocol, serverCodec),
	}
}

type counterServer struct {
	handler node.Handler[*counterprotocolv1.CounterInput, *counterprotocolv1.CounterOutput]
}

func (s *counterServer) Set(ctx context.Context, request *counterprotocolv1.SetRequest) (*counterprotocolv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", request))
	input := &counterprotocolv1.CounterInput{
		Input: &counterprotocolv1.CounterInput_Set{
			Set: request.SetInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Set",
			logging.Stringer("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterprotocolv1.SetResponse{
		Headers:   headers,
		SetOutput: output.GetSet(),
	}
	log.Debugw("Set",
		logging.Stringer("SetRequest", request),
		logging.Stringer("SetResponse", response))
	return response, nil
}

func (s *counterServer) Update(ctx context.Context, request *counterprotocolv1.UpdateRequest) (*counterprotocolv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request))
	input := &counterprotocolv1.CounterInput{
		Input: &counterprotocolv1.CounterInput_Update{
			Update: request.UpdateInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterprotocolv1.UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request),
		logging.Stringer("UpdateResponse", response))
	return response, nil
}

func (s *counterServer) Get(ctx context.Context, request *counterprotocolv1.GetRequest) (*counterprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	input := &counterprotocolv1.CounterInput{
		Input: &counterprotocolv1.CounterInput_Get{
			Get: request.GetInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", request),
		logging.Stringer("GetResponse", response))
	return response, nil
}

func (s *counterServer) Increment(ctx context.Context, request *counterprotocolv1.IncrementRequest) (*counterprotocolv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", request))
	input := &counterprotocolv1.CounterInput{
		Input: &counterprotocolv1.CounterInput_Increment{
			Increment: request.IncrementInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterprotocolv1.IncrementResponse{
		Headers:         headers,
		IncrementOutput: output.GetIncrement(),
	}
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", request),
		logging.Stringer("IncrementResponse", response))
	return response, nil
}

func (s *counterServer) Decrement(ctx context.Context, request *counterprotocolv1.DecrementRequest) (*counterprotocolv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", request))
	input := &counterprotocolv1.CounterInput{
		Input: &counterprotocolv1.CounterInput_Decrement{
			Decrement: request.DecrementInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterprotocolv1.DecrementResponse{
		Headers:         headers,
		DecrementOutput: output.GetDecrement(),
	}
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", request),
		logging.Stringer("DecrementResponse", response))
	return response, nil
}
