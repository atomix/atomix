// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
)

func newIndexedMapServer(proxies *primitive.Manager[indexedmapv1.IndexedMapServer]) indexedmapv1.IndexedMapServer {
	return &indexedMapServer{
		proxies: proxies,
	}
}

type indexedMapServer struct {
	proxies *primitive.Manager[indexedmapv1.IndexedMapServer]
}

func (s *indexedMapServer) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	proxy, err := s.proxies.Connect(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Create(ctx, request)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *indexedMapServer) Close(ctx context.Context, request *indexedmapv1.CloseRequest) (*indexedmapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	proxy, err := s.proxies.Close(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Close(ctx, request)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *indexedMapServer) Size(ctx context.Context, request *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Stringer("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Size(ctx, request)
	if err != nil {
		log.Warnw("Size",
			logging.Stringer("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Stringer("SizeResponse", response))
	return response, nil
}

func (s *indexedMapServer) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Stringer("AppendRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Append",
			logging.Stringer("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Append(ctx, request)
	if err != nil {
		log.Warnw("Append",
			logging.Stringer("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Append",
		logging.Stringer("AppendResponse", response))
	return response, nil
}

func (s *indexedMapServer) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Update(ctx, request)
	if err != nil {
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Update",
		logging.Stringer("UpdateResponse", response))
	return response, nil
}

func (s *indexedMapServer) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Get(ctx, request)
	if err != nil {
		log.Warnw("Get",
			logging.Stringer("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Stringer("GetResponse", response))
	return response, nil
}

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.FirstEntry(ctx, request)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.LastEntry(ctx, request)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.PrevEntry(ctx, request)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.NextEntry(ctx, request)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Remove(ctx, request)
	if err != nil {
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveResponse", response))
	return response, nil
}

func (s *indexedMapServer) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Clear(ctx, request)
	if err != nil {
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Stringer("ClearResponse", response))
	return response, nil
}

func (s *indexedMapServer) Events(request *indexedmapv1.EventsRequest, server indexedmapv1.IndexedMap_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", request))
	proxy, err := s.proxies.Get(server.Context())
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = proxy.Events(request, server)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *indexedMapServer) Entries(request *indexedmapv1.EntriesRequest, server indexedmapv1.IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", request))
	proxy, err := s.proxies.Get(server.Context())
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = proxy.Entries(request, server)
	if err != nil {
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ indexedmapv1.IndexedMapServer = (*indexedMapServer)(nil)
