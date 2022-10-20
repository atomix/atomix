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
		RegisterIndexedMapServer(server, NewIndexedMapServer(node))
	})
}

var serverCodec = node.NewCodec[*IndexedMapInput, *IndexedMapOutput](
	func(input *IndexedMapInput) ([]byte, error) {
		return proto.Marshal(input)
	},
	func(bytes []byte) (*IndexedMapOutput, error) {
		output := &IndexedMapOutput{}
		if err := proto.Unmarshal(bytes, output); err != nil {
			return nil, err
		}
		return output, nil
	})

func NewIndexedMapServer(protocol node.Protocol) IndexedMapServer {
	return &indexedMapServer{
		handler: node.NewHandler[*IndexedMapInput, *IndexedMapOutput](protocol, serverCodec),
	}
}

type indexedMapServer struct {
	handler node.Handler[*IndexedMapInput, *IndexedMapOutput]
}

func (s *indexedMapServer) Size(ctx context.Context, request *SizeRequest) (*SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Size_{
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

func (s *indexedMapServer) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	log.Debugw("Append",
		logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Append{
			Append: request.AppendInput,
		},
	}
	output, headers, err := s.handler.Propose(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &AppendResponse{
		Headers:      headers,
		AppendOutput: output.GetAppend(),
	}
	log.Debugw("Append",
		logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AppendResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Update(ctx context.Context, request *UpdateRequest) (*UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Update{
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

func (s *indexedMapServer) Remove(ctx context.Context, request *RemoveRequest) (*RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Remove{
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

func (s *indexedMapServer) Get(ctx context.Context, request *GetRequest) (*GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Get{
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

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *FirstEntryRequest) (*FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_FirstEntry{
			FirstEntry: request.FirstEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &FirstEntryResponse{
		Headers:          headers,
		FirstEntryOutput: output.GetFirstEntry(),
	}
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("FirstEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *LastEntryRequest) (*LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_LastEntry{
			LastEntry: request.LastEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &LastEntryResponse{
		Headers:         headers,
		LastEntryOutput: output.GetLastEntry(),
	}
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("LastEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *NextEntryRequest) (*NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_NextEntry{
			NextEntry: request.NextEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &NextEntryResponse{
		Headers:         headers,
		NextEntryOutput: output.GetNextEntry(),
	}
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("NextEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *PrevEntryRequest) (*PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_PrevEntry{
			PrevEntry: request.PrevEntryInput,
		},
	}
	output, headers, err := s.handler.Query(ctx, input, request.Headers)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response := &PrevEntryResponse{
		Headers:         headers,
		PrevEntryOutput: output.GetPrevEntry(),
	}
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PrevEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Clear(ctx context.Context, request *ClearRequest) (*ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Clear{
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

func (s *indexedMapServer) Events(request *EventsRequest, server IndexedMap_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Events{
			Events: request.EventsInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamProposalResponse[*IndexedMapOutput]]()
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

func (s *indexedMapServer) Entries(request *EntriesRequest, server IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)))
	input := &IndexedMapInput{
		Input: &IndexedMapInput_Entries{
			Entries: request.EntriesInput,
		},
	}

	stream := streams.NewBufferedStream[*node.StreamQueryResponse[*IndexedMapOutput]]()
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
