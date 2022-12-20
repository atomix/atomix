// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"container/list"
	"context"
	indexedmapv1 "github.com/atomix/atomix/api/pkg/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	indexedmapprotocolv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/indexedmap/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/protocols/rsm/pkg/client"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	indexedmapruntimev1 "github.com/atomix/atomix/runtime/pkg/runtime/indexedmap/v1"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"github.com/atomix/atomix/runtime/pkg/utils/stringer"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"io"
	"sync"
	"time"
)

var log = logging.GetLogger()

const truncLen = 200

func NewIndexedMap(protocol *client.Protocol, spec runtimev1.PrimitiveSpec) (indexedmapruntimev1.IndexedMap, error) {
	proxy := newIndexedMapClient(protocol, spec)
	var config indexedmapprotocolv1.IndexedMapConfig
	if err := spec.UnmarshalConfig(&config); err != nil {
		return nil, err
	}
	if config.Cache.Enabled {
		proxy = newCachingIndexedMapClient(proxy, config.Cache)
	}
	return proxy, nil
}

func newIndexedMapClient(protocol *client.Protocol, spec runtimev1.PrimitiveSpec) indexedmapv1.IndexedMapServer {
	return &indexedMapClient{
		Protocol:      protocol,
		PrimitiveSpec: spec,
	}
}

type indexedMapClient struct {
	*client.Protocol
	runtimev1.PrimitiveSpec
}

func (s *indexedMapClient) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.CreatePrimitive(ctx, s.PrimitiveMeta)
	})
	if err != nil {
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

func (s *indexedMapClient) Close(ctx context.Context, request *indexedmapv1.CloseRequest) (*indexedmapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	partitions := s.Partitions()
	err := async.IterAsync(len(partitions), func(i int) error {
		partition := partitions[i]
		session, err := partition.GetSession(ctx)
		if err != nil {
			return err
		}
		return session.ClosePrimitive(ctx, request.ID.Name)
	})
	if err != nil {
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

func (s *indexedMapClient) Size(ctx context.Context, request *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.SizeResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.SizeResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Size(ctx, &indexedmapprotocolv1.SizeRequest{
			Headers:   headers,
			SizeInput: &indexedmapprotocolv1.SizeInput{},
		})
	})
	if !ok {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Size",
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

func (s *indexedMapClient) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
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
	command := client.Proposal[*indexedmapprotocolv1.AppendResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.AppendResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Append(ctx, &indexedmapprotocolv1.AppendRequest{
			Headers: headers,
			AppendInput: &indexedmapprotocolv1.AppendInput{
				Key:   request.Key,
				Value: request.Value,
				TTL:   request.TTL,
			},
		})
	})
	if !ok {
		log.Warnw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.AppendResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Append",
		logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("AppendResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
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
	command := client.Proposal[*indexedmapprotocolv1.UpdateResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.UpdateResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Update(ctx, &indexedmapprotocolv1.UpdateRequest{
			Headers: headers,
			UpdateInput: &indexedmapprotocolv1.UpdateInput{
				Key:         request.Key,
				Index:       request.Index,
				Value:       request.Value,
				TTL:         request.TTL,
				PrevVersion: request.PrevVersion,
			},
		})
	})
	if !ok {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.UpdateResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.GetResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.GetResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Get(ctx, &indexedmapprotocolv1.GetRequest{
			Headers: headers,
			GetInput: &indexedmapprotocolv1.GetInput{
				Key:   request.Key,
				Index: request.Index,
			},
		})
	})
	if !ok {
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.GetResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) FirstEntry(ctx context.Context, request *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.FirstEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.FirstEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).FirstEntry(ctx, &indexedmapprotocolv1.FirstEntryRequest{
			Headers:         headers,
			FirstEntryInput: &indexedmapprotocolv1.FirstEntryInput{},
		})
	})
	if !ok {
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.FirstEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("FirstEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) LastEntry(ctx context.Context, request *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.LastEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.LastEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).LastEntry(ctx, &indexedmapprotocolv1.LastEntryRequest{
			Headers:        headers,
			LastEntryInput: &indexedmapprotocolv1.LastEntryInput{},
		})
	})
	if !ok {
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.LastEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("LastEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) NextEntry(ctx context.Context, request *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.NextEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.NextEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).NextEntry(ctx, &indexedmapprotocolv1.NextEntryRequest{
			Headers: headers,
			NextEntryInput: &indexedmapprotocolv1.NextEntryInput{
				Index: request.Index,
			},
		})
	})
	if !ok {
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.NextEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("NextEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) PrevEntry(ctx context.Context, request *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error) {
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
	query := client.Query[*indexedmapprotocolv1.PrevEntryResponse](primitive)
	output, ok, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (*indexedmapprotocolv1.PrevEntryResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).PrevEntry(ctx, &indexedmapprotocolv1.PrevEntryRequest{
			Headers: headers,
			PrevEntryInput: &indexedmapprotocolv1.PrevEntryInput{
				Index: request.Index,
			},
		})
	})
	if !ok {
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.PrevEntryResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("PrevEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
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
	command := client.Proposal[*indexedmapprotocolv1.RemoveResponse](primitive)
	output, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.RemoveResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Remove(ctx, &indexedmapprotocolv1.RemoveRequest{
			Headers: headers,
			RemoveInput: &indexedmapprotocolv1.RemoveInput{
				Key:         request.Key,
				Index:       request.Index,
				PrevVersion: request.PrevVersion,
			},
		})
	})
	if !ok {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &indexedmapv1.RemoveResponse{
		Entry: newClientEntry(output.Entry),
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapClient) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
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
	command := client.Proposal[*indexedmapprotocolv1.ClearResponse](primitive)
	_, ok, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (*indexedmapprotocolv1.ClearResponse, error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Clear(ctx, &indexedmapprotocolv1.ClearRequest{
			Headers:    headers,
			ClearInput: &indexedmapprotocolv1.ClearInput{},
		})
	})
	if !ok {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	} else if err != nil {
		log.Debugw("Clear",
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

func (s *indexedMapClient) Entries(request *indexedmapv1.EntriesRequest, server indexedmapv1.IndexedMap_EntriesServer) error {
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
	query := client.StreamQuery[*indexedmapprotocolv1.EntriesResponse](primitive)
	stream, err := query.Run(func(conn *grpc.ClientConn, headers *protocol.QueryRequestHeaders) (client.QueryStream[*indexedmapprotocolv1.EntriesResponse], error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Entries(server.Context(), &indexedmapprotocolv1.EntriesRequest{
			Headers: headers,
			EntriesInput: &indexedmapprotocolv1.EntriesInput{
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
		output, ok, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if !ok {
			log.Warnw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		} else if err != nil {
			log.Debugw("Entries",
				logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		}
		response := &indexedmapv1.EntriesResponse{
			Entry: *newClientEntry(&output.Entry),
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

func (s *indexedMapClient) Events(request *indexedmapv1.EventsRequest, server indexedmapv1.IndexedMap_EventsServer) error {
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
	command := client.StreamProposal[*indexedmapprotocolv1.EventsResponse](primitive)
	stream, err := command.Run(func(conn *grpc.ClientConn, headers *protocol.ProposalRequestHeaders) (client.ProposalStream[*indexedmapprotocolv1.EventsResponse], error) {
		return indexedmapprotocolv1.NewIndexedMapClient(conn).Events(server.Context(), &indexedmapprotocolv1.EventsRequest{
			Headers: headers,
			EventsInput: &indexedmapprotocolv1.EventsInput{
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
		output, ok, err := stream.Recv()
		if err == io.EOF {
			log.Debugw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.String("State", "Done"))
			return nil
		}
		if !ok {
			log.Warnw("Events",
				logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
				logging.Error("Error", err))
			return errors.ToProto(err)
		} else if err != nil {
			log.Debugw("Events",
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
		case *indexedmapprotocolv1.Event_Inserted_:
			response.Event.Event = &indexedmapv1.Event_Inserted_{
				Inserted: &indexedmapv1.Event_Inserted{
					Value: *newClientValue(&e.Inserted.Value),
				},
			}
		case *indexedmapprotocolv1.Event_Updated_:
			response.Event.Event = &indexedmapv1.Event_Updated_{
				Updated: &indexedmapv1.Event_Updated{
					Value:     *newClientValue(&e.Updated.Value),
					PrevValue: *newClientValue(&e.Updated.PrevValue),
				},
			}
		case *indexedmapprotocolv1.Event_Removed_:
			response.Event.Event = &indexedmapv1.Event_Removed_{
				Removed: &indexedmapv1.Event_Removed{
					Value:   *newClientValue(&e.Removed.Value),
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

func newClientEntry(entry *indexedmapprotocolv1.Entry) *indexedmapv1.Entry {
	if entry == nil {
		return nil
	}
	return &indexedmapv1.Entry{
		Key:   entry.Key,
		Index: entry.Index,
		Value: newClientValue(entry.Value),
	}
}

func newClientValue(value *indexedmapprotocolv1.Value) *indexedmapv1.VersionedValue {
	if value == nil {
		return nil
	}
	return &indexedmapv1.VersionedValue{
		Value:   value.Value,
		Version: value.Version,
	}
}

var _ indexedmapv1.IndexedMapServer = (*indexedMapClient)(nil)

func newCachingIndexedMapClient(m indexedmapv1.IndexedMapServer, config indexedmapprotocolv1.CacheConfig) indexedmapv1.IndexedMapServer {
	return &cachingIndexedMapClient{
		IndexedMapServer: m,
		config:           config,
		entries:          make(map[string]*list.Element),
		aged:             list.New(),
	}
}

type cachingIndexedMapClient struct {
	indexedmapv1.IndexedMapServer
	config  indexedmapprotocolv1.CacheConfig
	entries map[string]*list.Element
	aged    *list.List
	mu      sync.RWMutex
}

func (s *cachingIndexedMapClient) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
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

func (s *cachingIndexedMapClient) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	response, err := s.IndexedMapServer.Append(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(response.Entry)
	return response, nil
}

func (s *cachingIndexedMapClient) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
	response, err := s.IndexedMapServer.Update(ctx, request)
	if err != nil {
		return nil, err
	}
	s.update(response.Entry)
	return response, nil
}

func (s *cachingIndexedMapClient) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
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

func (s *cachingIndexedMapClient) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	response, err := s.IndexedMapServer.Remove(ctx, request)
	if err != nil {
		return nil, err
	}
	s.delete(request.Key)
	return response, nil
}

func (s *cachingIndexedMapClient) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
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

func (s *cachingIndexedMapClient) update(update *indexedmapv1.Entry) {
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

func (s *cachingIndexedMapClient) delete(key string) {
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

func (s *cachingIndexedMapClient) evict() {
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

var _ indexedmapv1.IndexedMapServer = (*cachingIndexedMapClient)(nil)

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

func newCachingEventsServer(server *cachingIndexedMapClient) indexedmapv1.IndexedMap_EventsServer {
	return &cachingEventsServer{
		server: server,
	}
}

type cachingEventsServer struct {
	grpc.ServerStream
	server *cachingIndexedMapClient
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
