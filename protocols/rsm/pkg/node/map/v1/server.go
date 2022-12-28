// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/map/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const truncLen = 200

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		mapprotocolv1.RegisterMapServer(server, NewMapServer(node))
	})
}

var serverCodec = node.NewCodec[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput](
	func(input *mapprotocolv1.MapInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*mapprotocolv1.MapOutput, error) {
		output := &mapprotocolv1.MapOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewMapServer(protocol node.Protocol) mapprotocolv1.MapServer {
	return &mapServer{
		handler: node.NewHandler[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput](protocol, serverCodec),
	}
}

type mapServer struct {
	handler node.Handler[*mapprotocolv1.MapInput, *mapprotocolv1.MapOutput]
}

func (s *mapServer) Size(ctx context.Context, request *mapprotocolv1.SizeRequest) (*mapprotocolv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Size_{
			Size_: request.SizeInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.SizeResponse{
		Headers:    headers,
		SizeOutput: output.GetSize_(),
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", request),
		logging.Stringer("SizeResponse", response))
	return response, nil
}

func (s *mapServer) Put(ctx context.Context, request *mapprotocolv1.PutRequest) (*mapprotocolv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Stringer("PutRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Put{
			Put: request.PutInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Put",
			logging.Stringer("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.PutResponse{
		Headers:   headers,
		PutOutput: output.GetPut(),
	}
	log.Debugw("Put",
		logging.Stringer("PutRequest", request),
		logging.Stringer("PutResponse", response))
	return response, nil
}

func (s *mapServer) Insert(ctx context.Context, request *mapprotocolv1.InsertRequest) (*mapprotocolv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Insert{
			Insert: request.InsertInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Insert",
			logging.Stringer("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.InsertResponse{
		Headers:      headers,
		InsertOutput: output.GetInsert(),
	}
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", request),
		logging.Stringer("InsertResponse", response))
	return response, nil
}

func (s *mapServer) Update(ctx context.Context, request *mapprotocolv1.UpdateRequest) (*mapprotocolv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Update{
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
	response := &mapprotocolv1.UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request),
		logging.Stringer("UpdateResponse", response))
	return response, nil
}

func (s *mapServer) Get(ctx context.Context, request *mapprotocolv1.GetRequest) (*mapprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Get{
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
	response := &mapprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", request),
		logging.Stringer("GetResponse", response))
	return response, nil
}

func (s *mapServer) Remove(ctx context.Context, request *mapprotocolv1.RemoveRequest) (*mapprotocolv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Remove{
			Remove: request.RemoveInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.RemoveResponse{
		Headers:      headers,
		RemoveOutput: output.GetRemove(),
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", request),
		logging.Stringer("RemoveResponse", response))
	return response, nil
}

func (s *mapServer) Clear(ctx context.Context, request *mapprotocolv1.ClearRequest) (*mapprotocolv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Clear{
			Clear: request.ClearInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.ClearResponse{
		Headers:     headers,
		ClearOutput: output.GetClear(),
	}
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", request),
		logging.Stringer("ClearResponse", response))
	return response, nil
}

func (s *mapServer) Lock(ctx context.Context, request *mapprotocolv1.LockRequest) (*mapprotocolv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Stringer("LockRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Lock{
			Lock: request.LockInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Lock",
			logging.Stringer("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.LockResponse{
		Headers:    headers,
		LockOutput: output.GetLock(),
	}
	log.Debugw("Lock",
		logging.Stringer("LockRequest", request),
		logging.Stringer("LockResponse", response))
	return response, nil
}

func (s *mapServer) Unlock(ctx context.Context, request *mapprotocolv1.UnlockRequest) (*mapprotocolv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Unlock{
			Unlock: request.UnlockInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Unlock",
			logging.Stringer("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.UnlockResponse{
		Headers:      headers,
		UnlockOutput: output.GetUnlock(),
	}
	log.Debugw("Unlock",
		logging.Stringer("UnlockRequest", request),
		logging.Stringer("UnlockResponse", response))
	return response, nil
}

func (s *mapServer) Events(request *mapprotocolv1.EventsRequest, server mapprotocolv1.Map_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Events{
			Events: request.EventsInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamProposalResponse[*mapprotocolv1.MapOutput]]()
	go func() {
		err := s.handler.StreamPropose(server.Context(), input, request.Headers, stream)
		if err != nil {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", request),
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
				logging.Stringer("EventsRequest", request),
				logging.Error("Error", result.Error))
			return result.Error
		}

		response := &mapprotocolv1.EventsResponse{
			Headers:      result.Value.Headers,
			EventsOutput: result.Value.Output.GetEvents(),
		}
		log.Debugw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Stringer("EventsResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *mapServer) Entries(request *mapprotocolv1.EntriesRequest, server mapprotocolv1.Map_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Entries{
			Entries: request.EntriesInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*mapprotocolv1.MapOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", request),
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
				logging.Stringer("EntriesRequest", request),
				logging.Error("Error", result.Error))
			return result.Error
		}

		response := &mapprotocolv1.EntriesResponse{
			Headers:       result.Value.Headers,
			EntriesOutput: result.Value.Output.GetEntries(),
		}
		log.Debugw("Entries",
			logging.Stringer("EntriesRequest", request),
			logging.Stringer("EntriesResponse", response))
		if err := server.Send(response); err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", request),
				logging.Error("Error", err))
			return err
		}
	}
}
