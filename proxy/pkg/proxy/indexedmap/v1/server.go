// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	indexedmapv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/stringer"
)

var log = logging.GetLogger()

const truncLen = 250

func newIndexedMapServer(delegate *proxy.Delegate[indexedmapv1.IndexedMapServer]) indexedmapv1.IndexedMapServer {
	return &indexedMapServer{
		delegate: delegate,
	}
}

type indexedMapServer struct {
	delegate *proxy.Delegate[indexedmapv1.IndexedMapServer]
}

func (s *indexedMapServer) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Create(request.ID.Name, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Close(ctx context.Context, request *indexedmapv1.CloseRequest) (*indexedmapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Size(ctx context.Context, request *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Size(ctx, request)
	if err != nil {
		log.Debugw("Size",
			logging.Stringer("SizeRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Stringer("SizeResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Append(ctx, request)
	if err != nil {
		log.Debugw("Append",
			logging.Stringer("AppendRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Append",
		logging.Stringer("AppendResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Update(ctx, request)
	if err != nil {
		log.Debugw("Update",
			logging.Stringer("UpdateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Update",
		logging.Stringer("UpdateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Get(ctx, request)
	if err != nil {
		log.Debugw("Get",
			logging.Stringer("GetRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Stringer("GetResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.FirstEntry(ctx, request)
	if err != nil {
		log.Debugw("FirstEntry",
			logging.Stringer("FirstEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("FirstEntry",
		logging.Stringer("FirstEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.LastEntry(ctx, request)
	if err != nil {
		log.Debugw("LastEntry",
			logging.Stringer("LastEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("LastEntry",
		logging.Stringer("LastEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.PrevEntry(ctx, request)
	if err != nil {
		log.Debugw("PrevEntry",
			logging.Stringer("PrevEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PrevEntry",
		logging.Stringer("PrevEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.NextEntry(ctx, request)
	if err != nil {
		log.Debugw("NextEntry",
			logging.Stringer("NextEntryRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("NextEntry",
		logging.Stringer("NextEntryResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Remove(ctx, request)
	if err != nil {
		log.Debugw("Remove",
			logging.Stringer("RemoveRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Stringer("RemoveResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Clear(ctx, request)
	if err != nil {
		log.Debugw("Clear",
			logging.Stringer("ClearRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Stringer("ClearResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *indexedMapServer) Events(request *indexedmapv1.EventsRequest, server indexedmapv1.IndexedMap_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Events(request, server)
	if err != nil {
		log.Debugw("Events",
			logging.Stringer("EventsRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *indexedMapServer) Entries(request *indexedmapv1.EntriesRequest, server indexedmapv1.IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Entries(request, server)
	if err != nil {
		log.Debugw("Entries",
			logging.Stringer("EntriesRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ indexedmapv1.IndexedMapServer = (*indexedMapServer)(nil)
