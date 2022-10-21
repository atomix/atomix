// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"container/list"
	"context"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"github.com/atomix/runtime/sdk/pkg/stringer"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

func NewIndexedMapProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) (indexedmapv1.IndexedMapServer, error) {
	proxy := newIndexedMapProxy(protocol, spec)
	var config IndexedMapConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return nil, err
	}
	if config.Cache.Enabled {
		proxy = newCachingIndexedMapProxy(proxy, config.Cache)
	}
	return proxy, nil
}

func newIndexedMapProxy(protocol *client.Protocol, spec runtime.PrimitiveSpec) indexedmapv1.IndexedMapServer {
	return &indexedMapProxy{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}
}

type indexedMapProxy struct {
	*client.Protocol
	runtime.PrimitiveSpec
}

func (s *indexedMapProxy) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.CreatePrimitive(ctx, s.PrimitiveSpec); err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.CreateResponse{}
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Close(ctx context.Context, request *indexedmapv1.CloseRequest) (*indexedmapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	if err := session.ClosePrimitive(ctx, request.ID.Name); err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Size(ctx context.Context, request *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*SizeResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*SizeResponse, error) {
		return NewIndexedMapClient(conn).Size(ctx, &SizeRequest{
			Headers:   headers,
			SizeInput: &SizeInput{},
		})
	})
	if err != nil {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.SizeResponse{
		Size_: output.Size_,
	}
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*AppendResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*AppendResponse, error) {
		return NewIndexedMapClient(conn).Append(ctx, &AppendRequest{
			Headers: headers,
			AppendInput: &AppendInput{
				Key:   request.Key,
				Value: request.Value,
				TTL:   request.TTL,
			},
		})
	})
	if err != nil {
		log.Warnw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.AppendResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("Append",
		logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AppendResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*UpdateResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*UpdateResponse, error) {
		return NewIndexedMapClient(conn).Update(ctx, &UpdateRequest{
			Headers: headers,
			UpdateInput: &UpdateInput{
				Key:         request.Key,
				Index:       request.Index,
				Value:       request.Value,
				TTL:         request.TTL,
				PrevVersion: request.PrevVersion,
			},
		})
	})
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.UpdateResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*GetResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*GetResponse, error) {
		return NewIndexedMapClient(conn).Get(ctx, &GetRequest{
			Headers: headers,
			GetInput: &GetInput{
				Key:   request.Key,
				Index: request.Index,
			},
		})
	})
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.GetResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) FirstEntry(ctx context.Context, request *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*FirstEntryResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*FirstEntryResponse, error) {
		return NewIndexedMapClient(conn).FirstEntry(ctx, &FirstEntryRequest{
			Headers:         headers,
			FirstEntryInput: &FirstEntryInput{},
		})
	})
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.FirstEntryResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("FirstEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) LastEntry(ctx context.Context, request *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*LastEntryResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*LastEntryResponse, error) {
		return NewIndexedMapClient(conn).LastEntry(ctx, &LastEntryRequest{
			Headers:        headers,
			LastEntryInput: &LastEntryInput{},
		})
	})
	if err != nil {
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.LastEntryResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("LastEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) NextEntry(ctx context.Context, request *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*NextEntryResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*NextEntryResponse, error) {
		return NewIndexedMapClient(conn).NextEntry(ctx, &NextEntryRequest{
			Headers: headers,
			NextEntryInput: &NextEntryInput{
				Index: request.Index,
			},
		})
	})
	if err != nil {
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.NextEntryResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("NextEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) PrevEntry(ctx context.Context, request *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	query := client.Query[*PrevEntryResponse](primitive)
	output, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*PrevEntryResponse, error) {
		return NewIndexedMapClient(conn).PrevEntry(ctx, &PrevEntryRequest{
			Headers: headers,
			PrevEntryInput: &PrevEntryInput{
				Index: request.Index,
			},
		})
	})
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.PrevEntryResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PrevEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*RemoveResponse](primitive)
	output, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*RemoveResponse, error) {
		return NewIndexedMapClient(conn).Remove(ctx, &RemoveRequest{
			Headers: headers,
			RemoveInput: &RemoveInput{
				Key:         request.Key,
				Index:       request.Index,
				PrevVersion: request.PrevVersion,
			},
		})
	})
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.RemoveResponse{
		Entry: newProxyEntry(output.Entry),
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(ctx)
	if err != nil {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	command := client.Proposal[*ClearResponse](primitive)
	_, err = command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*ClearResponse, error) {
		return NewIndexedMapClient(conn).Clear(ctx, &ClearRequest{
			Headers:    headers,
			ClearInput: &ClearInput{},
		})
	})
	if err != nil {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.ClearResponse{}
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapProxy) Entries(request *indexedmapv1.EntriesRequest, server indexedmapv1.IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	query := client.StreamQuery[*EntriesResponse](primitive)
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*EntriesResponse], error) {
		return NewIndexedMapClient(conn).Entries(server.Context(), &EntriesRequest{
			Headers: headers,
			EntriesInput: &EntriesInput{
				Watch: request.Watch,
			},
		})
	})
	if err != nil {
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	for {
		output, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		response := &indexedmapv1.EntriesResponse{
			Entry: *newProxyEntry(&output.Entry),
		}
		log.Debugw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("EntriesResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Stringer("EntriesResponse", stringer.Truncate(response, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}

func (s *indexedMapProxy) Events(request *indexedmapv1.EventsRequest, server indexedmapv1.IndexedMap_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)))
	partition := s.PartitionBy([]byte(request.ID.Name))
	session, err := partition.GetSession(server.Context())
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	primitive, err := session.GetPrimitive(request.ID.Name)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return errors.ToProto(err)
	}
	command := client.StreamProposal[*EventsResponse](primitive)
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*EventsResponse], error) {
		return NewIndexedMapClient(conn).Events(server.Context(), &EventsRequest{
			Headers: headers,
			EventsInput: &EventsInput{
				Key: request.Key,
			},
		})
	})
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	for {
		output, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.String("State", "Done"))
			return nil
		}
		if err != nil {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		response := &indexedmapv1.EventsResponse{
			Event: indexedmapv1.Event{
				Key:   output.Event.Key,
				Index: output.Event.Index,
			},
		}
		switch e := output.Event.Event.(type) {
		case *Event_Inserted_:
			response.Event.Event = &indexedmapv1.Event_Inserted_{
				Inserted: &indexedmapv1.Event_Inserted{
					Value: *newProxyValue(&e.Inserted.Value),
				},
			}
		case *Event_Updated_:
			response.Event.Event = &indexedmapv1.Event_Updated_{
				Updated: &indexedmapv1.Event_Updated{
					Value:     *newProxyValue(&e.Updated.Value),
					PrevValue: *newProxyValue(&e.Updated.PrevValue),
				},
			}
		case *Event_Removed_:
			response.Event.Event = &indexedmapv1.Event_Removed_{
				Removed: &indexedmapv1.Event_Removed{
					Value:   *newProxyValue(&e.Removed.Value),
					Expired: e.Removed.Expired,
				},
			}
		}
		log.Debugw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)))
		if err := server.Send(response); err != nil {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Stringer("EventsResponse", stringer.Truncate(response, truncLen)),
				logging.Error("Error", err))
			return err
		}
	}
}

func newProxyEntry(entry *Entry) *indexedmapv1.Entry {
	if entry == nil {
		return nil
	}
	return &indexedmapv1.Entry{
		Key:   entry.Key,
		Index: entry.Index,
		Value: newProxyValue(entry.Value),
	}
}

func newProxyValue(value *Value) *indexedmapv1.VersionedValue {
	if value == nil {
		return nil
	}
	return &indexedmapv1.VersionedValue{
		Value:   value.Value,
		Version: value.Version,
	}
}

var _ indexedmapv1.IndexedMapServer = (*indexedMapProxy)(nil)

func newCachingIndexedMapProxy(m indexedmapv1.IndexedMapServer, config CacheConfig) indexedmapv1.IndexedMapServer {
	return &cachingIndexedMapProxy{
		IndexedMapServer: m,
		config:           config,
		entries:          make(map[string]*list.Element),
		aged:             list.New(),
	}
}

type cachingIndexedMapProxy struct {
	indexedmapv1.IndexedMapServer
	config  CacheConfig
	entries map[string]*list.Element
	aged    *list.List
	mu      sync.RWMutex
}

func (s *cachingIndexedMapProxy) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
	response, err := s.IndexedMapServer.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	go func() {
		err = s.IndexedMapServer.Events(&indexedmapv1.EventsRequest{
			ID: request.ID,
		}, newCachingEventsServer(s))
		if err != nil {
			log.Error(err)
		}
	}()
	go func() {
		interval := s.config.EvictionInterval
		if interval == nil {
			i := time.Minute
			interval = &i
		}
		ticker := time.NewTicker(*interval)
		for range ticker.C {
			s.evict()
		}
	}()
	return response, nil
}

func (s *cachingIndexedMapProxy) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	response, err := s.IndexedMapServer.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(response.Entry)
	return response, nil
}

func (s *cachingIndexedMapProxy) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
	response, err := s.IndexedMapServer.Update(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(response.Entry)
	return response, nil
}

func (s *cachingIndexedMapProxy) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
	s.mu.RLock()
	elem, ok := s.entries[request.Key]
	s.mu.RUnlock()
	if ok {
		return &indexedmapv1.GetResponse{
			Entry: elem.Value.(*cachedEntry).entry,
		}, nil
	}
	return s.IndexedMapServer.Get(ctx, request)
}

func (s *cachingIndexedMapProxy) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	response, err := s.IndexedMapServer.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	s.delete(request.Key)
	return response, nil
}

func (s *cachingIndexedMapProxy) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
	response, err := s.IndexedMapServer.Clear(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.entries = make(map[string]*list.Element)
	s.aged = list.New()
	s.mu.Unlock()
	return response, nil
}

func (s *cachingIndexedMapProxy) update(update *indexedmapv1.Entry) {
	s.mu.RLock()
	check, ok := s.entries[update.Key]
	s.mu.RUnlock()
	if ok && check.Value.(*cachedEntry).entry.Value.Version >= update.Value.Version {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	check, ok = s.entries[update.Key]
	if ok && check.Value.(*cachedEntry).entry.Value.Version >= update.Value.Version {
		return
	}

	entry := newCachedEntry(update)
	if elem, ok := s.entries[update.Key]; ok {
		s.aged.Remove(elem)
	}
	s.entries[update.Key] = s.aged.PushBack(entry)
}

func (s *cachingIndexedMapProxy) delete(key string) {
	s.mu.RLock()
	_, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.entries[key]; ok {
		delete(s.entries, key)
		s.aged.Remove(elem)
	}
}

func (s *cachingIndexedMapProxy) evict() {
	t := time.Now()
	evictionDuration := time.Hour
	if s.config.EvictAfter != nil {
		evictionDuration = *s.config.EvictAfter
	}

	s.mu.RLock()
	size := uint64(len(s.entries))
	entry := s.aged.Front()
	s.mu.RUnlock()
	if (entry == nil || t.Sub(entry.Value.(*cachedEntry).timestamp) < evictionDuration) && size <= s.config.Size_ {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	size = uint64(len(s.entries))
	entry = s.aged.Front()
	if (entry == nil || t.Sub(entry.Value.(*cachedEntry).timestamp) < evictionDuration) && size <= s.config.Size_ {
		return
	}

	for i := s.aged.Len(); i > int(s.config.Size_); i-- {
		if entry := s.aged.Front(); entry != nil {
			s.aged.Remove(entry)
		}
	}

	entry = s.aged.Front()
	for entry != nil {
		if t.Sub(entry.Value.(*cachedEntry).timestamp) < evictionDuration {
			break
		}
		s.aged.Remove(entry)
		entry = s.aged.Front()
	}
}

var _ indexedmapv1.IndexedMapServer = (*cachingIndexedMapProxy)(nil)

func newChannelServer[T proto.Message](ch chan<- T) *channelServer[T] {
	return &channelServer[T]{
		ch: ch,
	}
}

type channelServer[T proto.Message] struct {
	grpc.ServerStream
	ch chan<- T
}

func (s *channelServer[T]) Send(response T) error {
	s.ch <- response
	return nil
}

func newCachingEventsServer(server *cachingIndexedMapProxy) indexedmapv1.IndexedMap_EventsServer {
	return &cachingEventsServer{
		server: server,
	}
}

type cachingEventsServer struct {
	grpc.ServerStream
	server *cachingIndexedMapProxy
}

func (s *cachingEventsServer) Send(response *indexedmapv1.EventsResponse) error {
	switch e := response.Event.Event.(type) {
	case *indexedmapv1.Event_Inserted_:
		s.server.update(&indexedmapv1.Entry{
			Key:   response.Event.Key,
			Index: response.Event.Index,
			Value: &indexedmapv1.VersionedValue{
				Value:   e.Inserted.Value.Value,
				Version: e.Inserted.Value.Version,
			},
		})
	case *indexedmapv1.Event_Updated_:
		s.server.update(&indexedmapv1.Entry{
			Key:   response.Event.Key,
			Index: response.Event.Index,
			Value: &indexedmapv1.VersionedValue{
				Value:   e.Updated.Value.Value,
				Version: e.Updated.Value.Version,
			},
		})
	case *indexedmapv1.Event_Removed_:
		s.server.delete(response.Event.Key)
	}
	return nil
}

func newCachedEntry(entry *indexedmapv1.Entry) *cachedEntry {
	return &cachedEntry{
		entry:     entry,
		timestamp: time.Now(),
	}
}

type cachedEntry struct {
	entry     *indexedmapv1.Entry
	timestamp time.Time
}
