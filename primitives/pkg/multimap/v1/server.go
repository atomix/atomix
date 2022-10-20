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
		RegisterMultiMapServer(server, NewMultiMapServer(node))
	})
}

var serverCodec = node.NewCodec[*MultiMapInput, *MultiMapOutput](
	func(input *MultiMapInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*MultiMapOutput, error) {
		output := &MultiMapOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewMultiMapServer(protocol node.Protocol) MultiMapServer {
	return &multiMapServer{
		handler: node.NewHandler[*MultiMapInput, *MultiMapOutput](protocol, serverCodec),
	}
}

type multiMapServer struct {
	handler node.Handler[*MultiMapInput, *MultiMapOutput]
}

func (s *multiMapServer) Size(ctx context.Context, request *SizeRequest) (*SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Size_{
			Size_: request.SizeInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &SizeResponse{
		Headers:    headers,
		SizeOutput: output.GetSize_(),
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) Put(ctx context.Context, request *PutRequest) (*PutResponse, error) {
	log.Debugw("Put",
		logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Put{
			Put: request.PutInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Put",
			logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &PutResponse{
		Headers:   headers,
		PutOutput: output.GetPut(),
	}
	log.Debugw("Put",
		logging.Stringer("PutRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) PutAll(ctx context.Context, request *PutAllRequest) (*PutAllResponse, error) {
	log.Debugw("PutAll",
		logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_PutAll{
			PutAll: request.PutAllInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PutAll",
			logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &PutAllResponse{
		Headers:      headers,
		PutAllOutput: output.GetPutAll(),
	}
	log.Debugw("PutAll",
		logging.Stringer("PutAllRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutAllResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) PutEntries(ctx context.Context, request *PutEntriesRequest) (*PutEntriesResponse, error) {
	log.Debugw("PutEntries",
		logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_PutEntries{
			PutEntries: request.PutEntriesInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PutEntries",
			logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &PutEntriesResponse{
		Headers:          headers,
		PutEntriesOutput: output.GetPutEntries(),
	}
	log.Debugw("PutEntries",
		logging.Stringer("PutEntriesRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PutEntriesResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) Replace(ctx context.Context, request *ReplaceRequest) (*ReplaceResponse, error) {
	log.Debugw("Replace",
		logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Replace{
			Replace: request.ReplaceInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Replace",
			logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &ReplaceResponse{
		Headers:       headers,
		ReplaceOutput: output.GetReplace(),
	}
	log.Debugw("Replace",
		logging.Stringer("ReplaceRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ReplaceResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) Contains(ctx context.Context, request *ContainsRequest) (*ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Contains{
			Contains: request.ContainsInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Contains",
			logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &ContainsResponse{
		Headers:        headers,
		ContainsOutput: output.GetContains(),
	}
	log.Debugw("Contains",
		logging.Stringer("ContainsRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ContainsResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Get{
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

func (s *multiMapServer) Remove(ctx context.Context, request *RemoveRequest) (*RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Remove{
			Remove: request.RemoveInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &RemoveResponse{
		Headers:      headers,
		RemoveOutput: output.GetRemove(),
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) RemoveAll(ctx context.Context, request *RemoveAllRequest) (*RemoveAllResponse, error) {
	log.Debugw("RemoveAll",
		logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_RemoveAll{
			RemoveAll: request.RemoveAllInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("RemoveAll",
			logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &RemoveAllResponse{
		Headers:         headers,
		RemoveAllOutput: output.GetRemoveAll(),
	}
	log.Debugw("RemoveAll",
		logging.Stringer("RemoveAllRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveAllResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) RemoveEntries(ctx context.Context, request *RemoveEntriesRequest) (*RemoveEntriesResponse, error) {
	log.Debugw("RemoveEntries",
		logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_RemoveEntries{
			RemoveEntries: request.RemoveEntriesInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("RemoveEntries",
			logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &RemoveEntriesResponse{
		Headers:             headers,
		RemoveEntriesOutput: output.GetRemoveEntries(),
	}
	log.Debugw("RemoveEntries",
		logging.Stringer("RemoveEntriesRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveEntriesResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) Clear(ctx context.Context, request *ClearRequest) (*ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Clear{
			Clear: request.ClearInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &ClearResponse{
		Headers:     headers,
		ClearOutput: output.GetClear(),
	}
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *multiMapServer) Events(request *EventsRequest, server MultiMap_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Events{
			Events: request.EventsInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamProposalResponse[*MultiMapOutput]]()
	go func() {
		err := s.handler.StreamPropose(server.Context(), input, request.Headers, stream)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
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
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}

		response := &EventsResponse{
			Headers:      result.Value.Headers,
			EventsOutput: result.Value.Output.GetEvents(),
		}
		log.Debugw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *multiMapServer) Entries(request *EntriesRequest, server MultiMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)))
	input := &MultiMapInput{
		Input: &MultiMapInput_Entries{
			Entries: request.EntriesInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*MultiMapOutput]]()
	go func() {
		err := s.handler.StreamQuery(server.Context(), input, request.Headers, stream)
		if err != nil {
			err = errors.ToProto(err)
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
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
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}

		response := &EntriesResponse{
			Headers:       result.Value.Headers,
			EntriesOutput: result.Value.Output.GetEntries(),
		}
		log.Debugw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("EntriesResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}
