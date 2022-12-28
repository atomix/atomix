// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	valueprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/value/v1"
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
		valueprotocolv1.RegisterValueServer(server, NewValueServer(node))
	})
}

var serverCodec = node.NewCodec[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput](
	func(input *valueprotocolv1.ValueInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*valueprotocolv1.ValueOutput, error) {
		output := &valueprotocolv1.ValueOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewValueServer(protocol node.Protocol) valueprotocolv1.ValueServer {
	return &valueServer{
		handler: node.NewHandler[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput](protocol, serverCodec),
	}
}

type valueServer struct {
	handler node.Handler[*valueprotocolv1.ValueInput, *valueprotocolv1.ValueOutput]
}

func (s *valueServer) Update(ctx context.Context, request *valueprotocolv1.UpdateRequest) (*valueprotocolv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Update{
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
	response := &valueprotocolv1.UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request),
		logging.Stringer("UpdateResponse", response))
	return response, nil
}

func (s *valueServer) Set(ctx context.Context, request *valueprotocolv1.SetRequest) (*valueprotocolv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Stringer("SetRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Set{
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
	response := &valueprotocolv1.SetResponse{
		Headers:   headers,
		SetOutput: output.GetSet(),
	}
	log.Debugw("Set",
		logging.Stringer("SetRequest", request),
		logging.Stringer("SetResponse", response))
	return response, nil
}

func (s *valueServer) Insert(ctx context.Context, request *valueprotocolv1.InsertRequest) (*valueprotocolv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Insert{
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
	response := &valueprotocolv1.InsertResponse{
		Headers:      headers,
		InsertOutput: output.GetInsert(),
	}
	log.Debugw("Insert",
		logging.Stringer("InsertRequest", request),
		logging.Stringer("InsertResponse", response))
	return response, nil
}

func (s *valueServer) Delete(ctx context.Context, request *valueprotocolv1.DeleteRequest) (*valueprotocolv1.DeleteResponse, error) {
	log.Debugw("Delete",
		logging.Stringer("DeleteRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Delete{
			Delete: request.DeleteInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Delete",
			logging.Stringer("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &valueprotocolv1.DeleteResponse{
		Headers:      headers,
		DeleteOutput: output.GetDelete(),
	}
	log.Debugw("Delete",
		logging.Stringer("DeleteRequest", request),
		logging.Stringer("DeleteResponse", response))
	return response, nil
}

func (s *valueServer) Get(ctx context.Context, request *valueprotocolv1.GetRequest) (*valueprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Get{
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
	response := &valueprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", request),
		logging.Stringer("GetResponse", response))
	return response, nil
}

func (s *valueServer) Events(request *valueprotocolv1.EventsRequest, server valueprotocolv1.Value_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Events{
			Events: request.EventsInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamProposalResponse[*valueprotocolv1.ValueOutput]]()
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

		response := &valueprotocolv1.EventsResponse{
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

func (s *valueServer) Watch(request *valueprotocolv1.WatchRequest, server valueprotocolv1.Value_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", request))
	input := &valueprotocolv1.ValueInput{
		Input: &valueprotocolv1.ValueInput_Watch{
			Watch: request.WatchInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*valueprotocolv1.ValueOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
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
			log.Warnw("Watch",
				logging.Stringer("WatchRequest", request),
				logging.Error("Error", result.Error))
			return result.Error
		}

		response := &valueprotocolv1.WatchResponse{
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
