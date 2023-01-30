// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type MapProxy interface {
	runtime.PrimitiveProxy
	// Size returns the size of the map
	Size(context.Context, *mapv1.SizeRequest) (*mapv1.SizeResponse, error)
	// Put puts an entry into the map
	Put(context.Context, *mapv1.PutRequest) (*mapv1.PutResponse, error)
	// Insert inserts an entry into the map
	Insert(context.Context, *mapv1.InsertRequest) (*mapv1.InsertResponse, error)
	// Update updates an entry in the map
	Update(context.Context, *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error)
	// Get gets the entry for a key
	Get(context.Context, *mapv1.GetRequest) (*mapv1.GetResponse, error)
	// Remove removes an entry from the map
	Remove(context.Context, *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error)
	// Clear removes all entries from the map
	Clear(context.Context, *mapv1.ClearRequest) (*mapv1.ClearResponse, error)
	// Lock locks a key in the map
	Lock(context.Context, *mapv1.LockRequest) (*mapv1.LockResponse, error)
	// Unlock unlocks a key in the map
	Unlock(context.Context, *mapv1.UnlockRequest) (*mapv1.UnlockResponse, error)
	// Events listens for change events
	Events(*mapv1.EventsRequest, mapv1.Map_EventsServer) error
	// Entries lists all entries in the map
	Entries(*mapv1.EntriesRequest, mapv1.Map_EntriesServer) error
}

func NewMapServer(rt *runtime.Runtime) mapv1.MapServer {
	return &mapServer{
		MapsServer: NewMapsServer(rt),
		primitives: runtime.NewPrimitiveRegistry[MapProxy](mapv1.PrimitiveType, rt),
	}
}

type mapServer struct {
	mapv1.MapsServer
	primitives runtime.PrimitiveRegistry[MapProxy]
}

func (s *mapServer) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
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

func (s *mapServer) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	log.Debugw("Put",
		logging.Trunc64("PutRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Put",
			logging.Trunc64("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Put(ctx, request)
	if err != nil {
		log.Debugw("Put",
			logging.Trunc64("PutRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Put",
		logging.Trunc64("PutResponse", response))
	return response, nil
}

func (s *mapServer) Insert(ctx context.Context, request *mapv1.InsertRequest) (*mapv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc64("InsertRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Insert",
			logging.Trunc64("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Insert(ctx, request)
	if err != nil {
		log.Debugw("Insert",
			logging.Trunc64("InsertRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Insert",
		logging.Trunc64("InsertResponse", response))
	return response, nil
}

func (s *mapServer) Update(ctx context.Context, request *mapv1.UpdateRequest) (*mapv1.UpdateResponse, error) {
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

func (s *mapServer) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
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

func (s *mapServer) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
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

func (s *mapServer) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
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

func (s *mapServer) Lock(ctx context.Context, request *mapv1.LockRequest) (*mapv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc64("LockRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Lock",
			logging.Trunc64("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Lock(ctx, request)
	if err != nil {
		log.Debugw("Lock",
			logging.Trunc64("LockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Lock",
		logging.Trunc64("LockResponse", response))
	return response, nil
}

func (s *mapServer) Unlock(ctx context.Context, request *mapv1.UnlockRequest) (*mapv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc64("UnlockRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Unlock",
			logging.Trunc64("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Unlock(ctx, request)
	if err != nil {
		log.Debugw("Unlock",
			logging.Trunc64("UnlockRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Unlock",
		logging.Trunc64("UnlockResponse", response))
	return response, nil
}

func (s *mapServer) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
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

func (s *mapServer) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
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

var _ mapv1.MapServer = (*mapServer)(nil)
