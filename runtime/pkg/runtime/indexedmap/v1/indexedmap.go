// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type IndexedMapProxy interface {
	runtime.PrimitiveProxy
	// Size returns the size of the map
	Size(context.Context, *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error)
	// Append appends an entry to the map
	Append(context.Context, *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error)
	// Update updates an entry in the map
	Update(context.Context, *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error)
	// Get gets the entry for a key
	Get(context.Context, *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error)
	// FirstEntry gets the first entry in the map
	FirstEntry(context.Context, *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error)
	// LastEntry gets the last entry in the map
	LastEntry(context.Context, *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error)
	// PrevEntry gets the previous entry in the map
	PrevEntry(context.Context, *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error)
	// NextEntry gets the next entry in the map
	NextEntry(context.Context, *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error)
	// Remove removes an entry from the map
	Remove(context.Context, *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error)
	// Clear removes all entries from the map
	Clear(context.Context, *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error)
	// Events listens for change events
	Events(*indexedmapv1.EventsRequest, indexedmapv1.IndexedMap_EventsServer) error
	// Entries lists all entries in the map
	Entries(*indexedmapv1.EntriesRequest, indexedmapv1.IndexedMap_EntriesServer) error
}

func NewIndexedMapServer(rt *runtime.Runtime) indexedmapv1.IndexedMapServer {
	return &indexedMapServer{
		IndexedMapsServer: NewIndexedMapsServer(rt),
		primitives:        runtime.NewPrimitiveRegistry[IndexedMapProxy](indexedmapv1.PrimitiveType, rt),
	}
}

type indexedMapServer struct {
	indexedmapv1.IndexedMapsServer
	primitives runtime.PrimitiveRegistry[IndexedMapProxy]
}

func (s *indexedMapServer) Size(ctx context.Context, request *indexedmapv1.SizeRequest) (*indexedmapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc64("SizeRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Size",
			logging.Trunc64("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Size(ctx, request)
	if err != nil {
		log.Debugw("Size",
			logging.Trunc64("SizeRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Size",
		logging.Trunc64("SizeResponse", response))
	return response, nil
}

func (s *indexedMapServer) Append(ctx context.Context, request *indexedmapv1.AppendRequest) (*indexedmapv1.AppendResponse, error) {
	log.Debugw("Append",
		logging.Trunc64("AppendRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Append",
			logging.Trunc64("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Append(ctx, request)
	if err != nil {
		log.Debugw("Append",
			logging.Trunc64("AppendRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Append",
		logging.Trunc64("AppendResponse", response))
	return response, nil
}

func (s *indexedMapServer) Update(ctx context.Context, request *indexedmapv1.UpdateRequest) (*indexedmapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc64("UpdateRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Update",
			logging.Trunc64("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Update(ctx, request)
	if err != nil {
		log.Debugw("Update",
			logging.Trunc64("UpdateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Update",
		logging.Trunc64("UpdateResponse", response))
	return response, nil
}

func (s *indexedMapServer) Get(ctx context.Context, request *indexedmapv1.GetRequest) (*indexedmapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc64("GetRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Get",
			logging.Trunc64("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Get(ctx, request)
	if err != nil {
		log.Debugw("Get",
			logging.Trunc64("GetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Get",
		logging.Trunc64("GetResponse", response))
	return response, nil
}

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *indexedmapv1.FirstEntryRequest) (*indexedmapv1.FirstEntryResponse, error) {
	log.Debugw("FirstEntry",
		logging.Trunc64("FirstEntryRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("FirstEntry",
			logging.Trunc64("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.FirstEntry(ctx, request)
	if err != nil {
		log.Debugw("FirstEntry",
			logging.Trunc64("FirstEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("FirstEntry",
		logging.Trunc64("FirstEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *indexedmapv1.LastEntryRequest) (*indexedmapv1.LastEntryResponse, error) {
	log.Debugw("LastEntry",
		logging.Trunc64("LastEntryRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("LastEntry",
			logging.Trunc64("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.LastEntry(ctx, request)
	if err != nil {
		log.Debugw("LastEntry",
			logging.Trunc64("LastEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("LastEntry",
		logging.Trunc64("LastEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *indexedmapv1.PrevEntryRequest) (*indexedmapv1.PrevEntryResponse, error) {
	log.Debugw("PrevEntry",
		logging.Trunc64("PrevEntryRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("PrevEntry",
			logging.Trunc64("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.PrevEntry(ctx, request)
	if err != nil {
		log.Debugw("PrevEntry",
			logging.Trunc64("PrevEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PrevEntry",
		logging.Trunc64("PrevEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *indexedmapv1.NextEntryRequest) (*indexedmapv1.NextEntryResponse, error) {
	log.Debugw("NextEntry",
		logging.Trunc64("NextEntryRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("NextEntry",
			logging.Trunc64("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.NextEntry(ctx, request)
	if err != nil {
		log.Debugw("NextEntry",
			logging.Trunc64("NextEntryRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("NextEntry",
		logging.Trunc64("NextEntryResponse", response))
	return response, nil
}

func (s *indexedMapServer) Remove(ctx context.Context, request *indexedmapv1.RemoveRequest) (*indexedmapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc64("RemoveRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Remove",
			logging.Trunc64("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Remove(ctx, request)
	if err != nil {
		log.Debugw("Remove",
			logging.Trunc64("RemoveRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Remove",
		logging.Trunc64("RemoveResponse", response))
	return response, nil
}

func (s *indexedMapServer) Clear(ctx context.Context, request *indexedmapv1.ClearRequest) (*indexedmapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc64("ClearRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Clear",
			logging.Trunc64("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Clear(ctx, request)
	if err != nil {
		log.Debugw("Clear",
			logging.Trunc64("ClearRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Clear",
		logging.Trunc64("ClearResponse", response))
	return response, nil
}

func (s *indexedMapServer) Events(request *indexedmapv1.EventsRequest, server indexedmapv1.IndexedMap_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc64("EventsRequest", request),
		logging.String("State", "started"))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Events",
			logging.Trunc64("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Events(request, server)
	if err != nil {
		log.Debugw("Events",
			logging.Trunc64("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *indexedMapServer) Entries(request *indexedmapv1.EntriesRequest, server indexedmapv1.IndexedMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc64("EntriesRequest", request),
		logging.String("State", "started"))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Entries",
			logging.Trunc64("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Entries(request, server)
	if err != nil {
		log.Debugw("Entries",
			logging.Trunc64("EntriesRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ indexedmapv1.IndexedMapServer = (*indexedMapServer)(nil)
