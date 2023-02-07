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
		logging.Trunc128("SizeRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Size_{
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
	response := &mapprotocolv1.SizeResponse{
		Headers:    headers,
		SizeOutput: output.GetSize_(),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *mapServer) Put(ctx context.Context, request *mapprotocolv1.PutRequest) (*mapprotocolv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Trunc128("PutRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Put{
			Put: request.PutInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Put",
			logging.Trunc128("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.PutResponse{
		Headers:   headers,
		PutOutput: output.GetPut(),
	}
	log.Debugw("Put",
		logging.Trunc128("PutRequest", request),
		logging.Trunc128("PutResponse", response))
	return response, nil
}

func (s *mapServer) Insert(ctx context.Context, request *mapprotocolv1.InsertRequest) (*mapprotocolv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Insert{
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
	response := &mapprotocolv1.InsertResponse{
		Headers:      headers,
		InsertOutput: output.GetInsert(),
	}
	log.Debugw("Insert",
		logging.Trunc128("InsertRequest", request),
		logging.Trunc128("InsertResponse", response))
	return response, nil
}

func (s *mapServer) Update(ctx context.Context, request *mapprotocolv1.UpdateRequest) (*mapprotocolv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Update{
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
	response := &mapprotocolv1.UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *mapServer) Get(ctx context.Context, request *mapprotocolv1.GetRequest) (*mapprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Get{
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
	response := &mapprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *mapServer) Remove(ctx context.Context, request *mapprotocolv1.RemoveRequest) (*mapprotocolv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Remove{
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
	response := &mapprotocolv1.RemoveResponse{
		Headers:      headers,
		RemoveOutput: output.GetRemove(),
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *mapServer) Clear(ctx context.Context, request *mapprotocolv1.ClearRequest) (*mapprotocolv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Clear{
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
	response := &mapprotocolv1.ClearResponse{
		Headers:     headers,
		ClearOutput: output.GetClear(),
	}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *mapServer) Lock(ctx context.Context, request *mapprotocolv1.LockRequest) (*mapprotocolv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Lock{
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
	response := &mapprotocolv1.LockResponse{
		Headers:    headers,
		LockOutput: output.GetLock(),
	}
	log.Debugw("Lock",
		logging.Trunc128("LockRequest", request),
		logging.Trunc128("LockResponse", response))
	return response, nil
}

func (s *mapServer) Unlock(ctx context.Context, request *mapprotocolv1.UnlockRequest) (*mapprotocolv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Unlock{
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
	response := &mapprotocolv1.UnlockResponse{
		Headers:      headers,
		UnlockOutput: output.GetUnlock(),
	}
	log.Debugw("Unlock",
		logging.Trunc128("UnlockRequest", request),
		logging.Trunc128("UnlockResponse", response))
	return response, nil
}

func (s *mapServer) Prepare(ctx context.Context, request *mapprotocolv1.PrepareRequest) (*mapprotocolv1.PrepareResponse, error) {
	log.Debugw("Prepare",
		logging.Trunc128("PrepareRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Prepare{
			Prepare: request.PrepareInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Prepare",
			logging.Trunc128("PrepareRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.PrepareResponse{
		Headers:       headers,
		PrepareOutput: output.GetPrepare(),
	}
	log.Debugw("Prepare",
		logging.Trunc128("PrepareRequest", request),
		logging.Trunc128("PrepareResponse", response))
	return response, nil
}

func (s *mapServer) Commit(ctx context.Context, request *mapprotocolv1.CommitRequest) (*mapprotocolv1.CommitResponse, error) {
	log.Debugw("Commit",
		logging.Trunc128("CommitRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Commit{
			Commit: request.CommitInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Commit",
			logging.Trunc128("CommitRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.CommitResponse{
		Headers:      headers,
		CommitOutput: output.GetCommit(),
	}
	log.Debugw("Commit",
		logging.Trunc128("CommitRequest", request),
		logging.Trunc128("CommitResponse", response))
	return response, nil
}

func (s *mapServer) Abort(ctx context.Context, request *mapprotocolv1.AbortRequest) (*mapprotocolv1.AbortResponse, error) {
	log.Debugw("Abort",
		logging.Trunc128("AbortRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Abort{
			Abort: request.AbortInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Abort",
			logging.Trunc128("AbortRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.AbortResponse{
		Headers:     headers,
		AbortOutput: output.GetAbort(),
	}
	log.Debugw("Abort",
		logging.Trunc128("AbortRequest", request),
		logging.Trunc128("AbortResponse", response))
	return response, nil
}

func (s *mapServer) Apply(ctx context.Context, request *mapprotocolv1.ApplyRequest) (*mapprotocolv1.ApplyResponse, error) {
	log.Debugw("Apply",
		logging.Trunc128("ApplyRequest", request))
	input := &mapprotocolv1.MapInput{
		Input: &mapprotocolv1.MapInput_Apply{
			Apply: request.ApplyInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Apply",
			logging.Trunc128("ApplyRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapprotocolv1.ApplyResponse{
		Headers:     headers,
		ApplyOutput: output.GetApply(),
	}
	log.Debugw("Apply",
		logging.Trunc128("ApplyRequest", request),
		logging.Trunc128("ApplyResponse", response))
	return response, nil
}

func (s *mapServer) Events(request *mapprotocolv1.EventsRequest, server mapprotocolv1.Map_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc128("EventsRequest", request))
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

		response := &mapprotocolv1.EventsResponse{
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

func (s *mapServer) Entries(request *mapprotocolv1.EntriesRequest, server mapprotocolv1.Map_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc128("EntriesRequest", request))
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

		response := &mapprotocolv1.EntriesResponse{
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
