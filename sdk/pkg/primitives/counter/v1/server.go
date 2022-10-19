// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"github.com/gogo/protobuf/proto"
)

var serverCodec = node.NewCodec[*CounterInput, *CounterOutput](
	func(input *CounterInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*CounterOutput, error) {
		output := &CounterOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewCounterServer(protocol node.Protocol) CounterServer {
	return &counterServer{
		handler: node.NewHandler[*CounterInput, *CounterOutput](protocol, serverCodec),
	}
}

type counterServer struct {
	handler node.Handler[*CounterInput, *CounterOutput]
}

func (s *counterServer) Set(ctx context.Context, request *SetRequest) (*SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)))
	input := &CounterInput{
		Input: &CounterInput_Set{
			Set: request.SetInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Set",
			logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &SetResponse{
		Headers:   headers,
		SetOutput: output.GetSet(),
	}
	log.Debugw("Set",
		logging.Stringer("SetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterServer) Update(ctx context.Context, request *UpdateRequest) (*UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	input := &CounterInput{
		Input: &CounterInput_Update{
			Update: request.UpdateInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterServer) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	input := &CounterInput{
		Input: &CounterInput_Get{
			Get: request.GetInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterServer) Increment(ctx context.Context, request *IncrementRequest) (*IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)))
	input := &CounterInput{
		Input: &CounterInput_Increment{
			Increment: request.IncrementInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Increment",
			logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &IncrementResponse{
		Headers:         headers,
		IncrementOutput: output.GetIncrement(),
	}
	log.Debugw("Increment",
		logging.Stringer("IncrementRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("IncrementResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *counterServer) Decrement(ctx context.Context, request *DecrementRequest) (*DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)))
	input := &CounterInput{
		Input: &CounterInput_Decrement{
			Decrement: request.DecrementInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Decrement",
			logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &DecrementResponse{
		Headers:         headers,
		DecrementOutput: output.GetDecrement(),
	}
	log.Debugw("Decrement",
		logging.Stringer("DecrementRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("DecrementResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}
