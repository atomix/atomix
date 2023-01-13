// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapprotocolv1 "github.com/atomix/atomix/protocols/rsm/api/indexedmap/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/node"
	"github.com/atomix/atomix/runtime/pkg/logging"
	streams "github.com/atomix/atomix/runtime/pkg/stream"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

func RegisterServer(node *node.Node) {
	node.RegisterService(func(server *grpc.Server) {
		indexedmapprotocolv1.RegisterIndexedMapServer(server, NewIndexedMapServer(node))
	})
}

var serverCodec = node.NewCodec[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput](
	func(input *indexedmapprotocolv1.IndexedMapInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*indexedmapprotocolv1.IndexedMapOutput, error) {
		output := &indexedmapprotocolv1.IndexedMapOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewIndexedMapServer(protocol node.Protocol) indexedmapprotocolv1.IndexedMapServer {
	return &indexedMapServer{
		handler: node.NewHandler[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput](protocol, serverCodec),
	}
}

type indexedMapServer struct {
	handler node.Handler[*indexedmapprotocolv1.IndexedMapInput, *indexedmapprotocolv1.IndexedMapOutput]
}

func (s *indexedMapServer) Size(ctx context.Context, request *indexedmapprotocolv1.SizeRequest) (*indexedmapprotocolv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Size_{
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
	response := &indexedmapprotocolv1.SizeResponse{
		Headers:    headers,
		SizeOutput: output.GetSize_(),
	}
	log.Debugw("Size",
		logging.Trunc128("SizeRequest", request),
		logging.Trunc128("SizeResponse", response))
	return response, nil
}

func (s *indexedMapServer) Append(ctx context.Context, request *indexedmapprotocolv1.AppendRequest) (*indexedmapprotocolv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Trunc128("AppendRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Append{
			Append: request.AppendInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("Append",
			logging.Trunc128("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapprotocolv1.AppendResponse{
		Headers:      headers,
		AppendOutput: output.GetAppend(),
	}
	log.Debugw("Append",
		logging.Trunc128("AppendRequest", request),
		logging.Trunc128("AppendResponse", response))
	return response, nil
}

func (s *indexedMapServer) Update(ctx context.Context, request *indexedmapprotocolv1.UpdateRequest) (*indexedmapprotocolv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Update{
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
	response := &indexedmapprotocolv1.UpdateResponse{
		Headers:      headers,
		UpdateOutput: output.GetUpdate(),
	}
	log.Debugw("Update",
		logging.Trunc128("UpdateRequest", request),
		logging.Trunc128("UpdateResponse", response))
	return response, nil
}

func (s *indexedMapServer) Remove(ctx context.Context, request *indexedmapprotocolv1.RemoveRequest) (*indexedmapprotocolv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Remove{
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
	response := &indexedmapprotocolv1.RemoveResponse{
		Headers:      headers,
		RemoveOutput: output.GetRemove(),
	}
	log.Debugw("Remove",
		logging.Trunc128("RemoveRequest", request),
		logging.Trunc128("RemoveResponse", response))
	return response, nil
}

func (s *indexedMapServer) Get(ctx context.Context, request *indexedmapprotocolv1.GetRequest) (*indexedmapprotocolv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Get{
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
	response := &indexedmapprotocolv1.GetResponse{
		Headers:   headers,
		GetOutput: output.GetGet(),
	}
	log.Debugw("Get",
		logging.Trunc128("GetRequest", request),
		logging.Trunc128("GetResponse", response))
	return response, nil
}

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *indexedmapprotocolv1.FirstEntryRequest) (*indexedmapprotocolv1.FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Trunc128("FirstEntryRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_FirstEntry{
			FirstEntry: request.FirstEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Trunc128("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapprotocolv1.FirstEntryResponse{
		Headers:          headers,
		FirstEntryOutput: output.GetFirstEntry(),
	}
	log.Debugw("FirstEntry",
		logging.Trunc128("FirstEntryRequest", request),
		logging.Trunc128("FirstEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *indexedmapprotocolv1.LastEntryRequest) (*indexedmapprotocolv1.LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Trunc128("LastEntryRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_LastEntry{
			LastEntry: request.LastEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Trunc128("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapprotocolv1.LastEntryResponse{
		Headers:         headers,
		LastEntryOutput: output.GetLastEntry(),
	}
	log.Debugw("LastEntry",
		logging.Trunc128("LastEntryRequest", request),
		logging.Trunc128("LastEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *indexedmapprotocolv1.NextEntryRequest) (*indexedmapprotocolv1.NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Trunc128("NextEntryRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_NextEntry{
			NextEntry: request.NextEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Trunc128("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapprotocolv1.NextEntryResponse{
		Headers:         headers,
		NextEntryOutput: output.GetNextEntry(),
	}
	log.Debugw("NextEntry",
		logging.Trunc128("NextEntryRequest", request),
		logging.Trunc128("NextEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *indexedmapprotocolv1.PrevEntryRequest) (*indexedmapprotocolv1.PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Trunc128("PrevEntryRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_PrevEntry{
			PrevEntry: request.PrevEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Trunc128("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapprotocolv1.PrevEntryResponse{
		Headers:         headers,
		PrevEntryOutput: output.GetPrevEntry(),
	}
	log.Debugw("PrevEntry",
		logging.Trunc128("PrevEntryRequest", request),
		logging.Trunc128("PrevEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) Clear(ctx context.Context, request *indexedmapprotocolv1.ClearRequest) (*indexedmapprotocolv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Clear{
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
	response := &indexedmapprotocolv1.ClearResponse{
		Headers:     headers,
		ClearOutput: output.GetClear(),
	}
	log.Debugw("Clear",
		logging.Trunc128("ClearRequest", request),
		logging.Trunc128("ClearResponse", response))
	return response, nil
}

func (s *indexedMapServer) Events(request *indexedmapprotocolv1.EventsRequest, server indexedmapprotocolv1.IndexedMap_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc128("EventsRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Events{
			Events: request.EventsInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamProposalResponse[*indexedmapprotocolv1.IndexedMapOutput]]()
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

		response := &indexedmapprotocolv1.EventsResponse{
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

func (s *indexedMapServer) Entries(request *indexedmapprotocolv1.EntriesRequest, server indexedmapprotocolv1.IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc128("EntriesRequest", request))
	input := &indexedmapprotocolv1.IndexedMapInput{
		Input: &indexedmapprotocolv1.IndexedMapInput_Entries{
			Entries: request.EntriesInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*indexedmapprotocolv1.IndexedMapOutput]]()
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

		response := &indexedmapprotocolv1.EntriesResponse{
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
