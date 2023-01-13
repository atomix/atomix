// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	countermapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/countermap/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		countermapprotocolv1.RegisterCounterMapServer(server, NewCounterMapServer(node))
	})
}

var serverCodec = node.NewCodec[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput](
	func(input *countermapprotocolv1.CounterMapInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*countermapprotocolv1.CounterMapOutput, error) {
		output := &countermapprotocolv1.CounterMapOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewCounterMapServer(protocol node.Protocol) countermapprotocolv1.CounterMapServer {
	return &counterMapServer{
		handler: node.NewHandler[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput](protocol, serverCodec),
	}
}

type counterMapServer struct {
	handler node.Handler[*countermapprotocolv1.CounterMapInput, *countermapprotocolv1.CounterMapOutput]
}

func (s *counterMapServer) Size(ctx context.Context, request *countermapprotocolv1.SizeRequest) (*countermapprotocolv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Size_{
			Size_: request.SizeInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Size",
			logging.Trunc128("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.SizeResponse{
		Headers:    headers,
		SizeOutput: output.GetSize_(),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *counterMapServer) Set(ctx context.Context, request *countermapprotocolv1.SetRequest) (*countermapprotocolv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Set{
			Set: request.SetInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Set",
			logging.Trunc128("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.SetResponse{
		Headers:   headers,
		SetOutput: output.GetSet(),
	}
	log.Debugw("Set",
		logging.Trunc128("SetRequest", request),
		logging.Trunc128("SetResponse", response))
	return response, nil
}

func (s *counterMapServer) Insert(ctx context.Context, request *countermapprotocolv1.InsertRequest) (*countermapprotocolv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Insert{
			Insert: request.InsertInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Insert",
			logging.Trunc128("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.InsertResponse{
		Headers:      headers,
		InsertOutput: output.GetInsert(),
	}
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request),
		logging.Trunc128("InsertResponse", response))
	return response, nil
}

func (s *counterMapServer) Update(ctx context.Context, request *countermapprotocolv1.UpdateRequest) (*countermapprotocolv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Update{
			Update: request.UpdateInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Update",
			logging.Trunc128("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *counterMapServer) Increment(ctx context.Context, request *countermapprotocolv1.IncrementRequest) (*countermapprotocolv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Trunc128("IncrementRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Increment{
			Increment: request.IncrementInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Increment",
			logging.Trunc128("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.IncrementResponse{
		Headers:         headers,
		IncrementOutput: output.GetIncrement(),
	}
	log.Debugw("Increment",
		logging.Trunc128("IncrementRequest", request),
		logging.Trunc128("IncrementResponse", response))
	return response, nil
}

func (s *counterMapServer) Decrement(ctx context.Context, request *countermapprotocolv1.DecrementRequest) (*countermapprotocolv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Trunc128("DecrementRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Decrement{
			Decrement: request.DecrementInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Decrement",
			logging.Trunc128("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.DecrementResponse{
		Headers:         headers,
		DecrementOutput: output.GetDecrement(),
	}
	log.Debugw("Decrement",
		logging.Trunc128("DecrementRequest", request),
		logging.Trunc128("DecrementResponse", response))
	return response, nil
}

func (s *counterMapServer) Get(ctx context.Context, request *countermapprotocolv1.GetRequest) (*countermapprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Get{
			Get: request.GetInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc128("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *counterMapServer) Remove(ctx context.Context, request *countermapprotocolv1.RemoveRequest) (*countermapprotocolv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Remove{
			Remove: request.RemoveInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Remove",
			logging.Trunc128("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.RemoveResponse{
		Headers:      headers,
		RemoveOutput: output.GetRemove(),
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *counterMapServer) Clear(ctx context.Context, request *countermapprotocolv1.ClearRequest) (*countermapprotocolv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Clear{
			Clear: request.ClearInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Clear",
			logging.Trunc128("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.ClearResponse{
		Headers:     headers,
		ClearOutput: output.GetClear(),
	}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *counterMapServer) Lock(ctx context.Context, request *countermapprotocolv1.LockRequest) (*countermapprotocolv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Lock{
			Lock: request.LockInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Lock",
			logging.Trunc128("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.LockResponse{
		Headers:    headers,
		LockOutput: output.GetLock(),
	}
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request),
		logging.Trunc128("LockResponse", response))
	return response, nil
}

func (s *counterMapServer) Unlock(ctx context.Context, request *countermapprotocolv1.UnlockRequest) (*countermapprotocolv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Unlock{
			Unlock: request.UnlockInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Unlock",
			logging.Trunc128("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapprotocolv1.UnlockResponse{
		Headers:      headers,
		UnlockOutput: output.GetUnlock(),
	}
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request),
		logging.Trunc128("UnlockResponse", response))
	return response, nil
}

func (s *counterMapServer) Events(request *countermapprotocolv1.EventsRequest, server countermapprotocolv1.CounterMap_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc128("EventsRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Events{
			Events: request.EventsInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamProposalResponse[*countermapprotocolv1.CounterMapOutput]]()
	go func() {
		err := s.handler.StreamPropose(server.Context(), input, request.Headers, stream)
		if err != nil {
			log.Warnw("Events",
				logging.Trunc128("EventsRequest", request),
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
			log.Warnw("Events",
				logging.Trunc128("EventsRequest", request),
				logging.Error("Error", result.Error))
			return result.Error
		}

		response := &countermapprotocolv1.EventsResponse{
			Headers:      result.Value.Headers,
			EventsOutput: result.Value.Output.GetEvents(),
		}
		log.Debugw("Events",
			logging.Trunc128("EventsRequest", request),
			logging.Trunc128("EventsResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Events",
				logging.Trunc128("EventsRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *counterMapServer) Entries(request *countermapprotocolv1.EntriesRequest, server countermapprotocolv1.CounterMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc128("EntriesRequest", request))
	input := &countermapprotocolv1.CounterMapInput{
		Input: &countermapprotocolv1.CounterMapInput_Entries{
			Entries: request.EntriesInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*countermapprotocolv1.CounterMapOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
			log.Warnw("Entries",
				logging.Trunc128("EntriesRequest", request),
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
			log.Warnw("Entries",
				logging.Trunc128("EntriesRequest", request),
				logging.Error("Error", result.Error))
			return result.Error
		}

		response := &countermapprotocolv1.EntriesResponse{
			Headers:       result.Value.Headers,
			EntriesOutput: result.Value.Output.GetEntries(),
		}
		log.Debugw("Entries",
			logging.Trunc128("EntriesRequest", request),
			logging.Trunc128("EntriesResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Entries",
				logging.Trunc128("EntriesRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}
