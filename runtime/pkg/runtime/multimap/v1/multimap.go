// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	multimapv1 "github.com/atomix/atomix/api/runtime/multimap/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type MultiMapProxy interface {
	runtime.PrimitiveProxy
	// Size returns the size of the map
	Size(context.Context, *multimapv1.SizeRequest) (*multimapv1.SizeResponse, error)
	// Put adds a value to an entry in the map
	Put(context.Context, *multimapv1.PutRequest) (*multimapv1.PutResponse, error)
	// PutAll adds values to an entry in the map
	PutAll(context.Context, *multimapv1.PutAllRequest) (*multimapv1.PutAllResponse, error)
	// PutEntries adds entries to the map
	PutEntries(context.Context, *multimapv1.PutEntriesRequest) (*multimapv1.PutEntriesResponse, error)
	// Replace replaces the values of an entry in the map
	Replace(context.Context, *multimapv1.ReplaceRequest) (*multimapv1.ReplaceResponse, error)
	// Contains checks if an entry exists in the map
	Contains(context.Context, *multimapv1.ContainsRequest) (*multimapv1.ContainsResponse, error)
	// Get gets the entry for a key
	Get(context.Context, *multimapv1.GetRequest) (*multimapv1.GetResponse, error)
	// Remove removes an entry from the map
	Remove(context.Context, *multimapv1.RemoveRequest) (*multimapv1.RemoveResponse, error)
	// RemoveAll removes a key from the map
	RemoveAll(context.Context, *multimapv1.RemoveAllRequest) (*multimapv1.RemoveAllResponse, error)
	// RemoveEntries removes entries from the map
	RemoveEntries(context.Context, *multimapv1.RemoveEntriesRequest) (*multimapv1.RemoveEntriesResponse, error)
	// Clear removes all entries from the map
	Clear(context.Context, *multimapv1.ClearRequest) (*multimapv1.ClearResponse, error)
	// Events listens for change events
	Events(*multimapv1.EventsRequest, multimapv1.MultiMap_EventsServer) error
	// Entries lists all entries in the map
	Entries(*multimapv1.EntriesRequest, multimapv1.MultiMap_EntriesServer) error
}

func NewMultiMapServer(rt *runtime.Runtime) multimapv1.MultiMapServer {
	return &multiMapServer{
		MultiMapsServer: NewMultiMapsServer(rt),
		primitives:      runtime.NewPrimitiveRegistry[MultiMapProxy](multimapv1.PrimitiveType, rt),
	}
}

type multiMapServer struct {
	multimapv1.MultiMapsServer
	primitives runtime.PrimitiveRegistry[MultiMapProxy]
}

func (s *multiMapServer) Size(ctx context.Context, request *multimapv1.SizeRequest) (*multimapv1.SizeResponse, error) {
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

func (s *multiMapServer) Put(ctx context.Context, request *multimapv1.PutRequest) (*multimapv1.PutResponse, error) {
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

func (s *multiMapServer) PutAll(ctx context.Context, request *multimapv1.PutAllRequest) (*multimapv1.PutAllResponse, error) {
	log.Debugw("PutAll",
		logging.Trunc64("PutAllRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("PutAll",
			logging.Trunc64("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.PutAll(ctx, request)
	if err != nil {
		log.Debugw("PutAll",
			logging.Trunc64("PutAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PutAll",
		logging.Trunc64("PutAllResponse", response))
	return response, nil
}

func (s *multiMapServer) PutEntries(ctx context.Context, request *multimapv1.PutEntriesRequest) (*multimapv1.PutEntriesResponse, error) {
	log.Debugw("PutEntries",
		logging.Trunc64("PutEntriesRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("PutEntries",
			logging.Trunc64("PutEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.PutEntries(ctx, request)
	if err != nil {
		log.Debugw("PutEntries",
			logging.Trunc64("PutEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("PutEntries",
		logging.Trunc64("PutEntriesResponse", response))
	return response, nil
}

func (s *multiMapServer) Replace(ctx context.Context, request *multimapv1.ReplaceRequest) (*multimapv1.ReplaceResponse, error) {
	log.Debugw("Replace",
		logging.Trunc64("ReplaceRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Replace",
			logging.Trunc64("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Replace(ctx, request)
	if err != nil {
		log.Debugw("Replace",
			logging.Trunc64("ReplaceRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Replace",
		logging.Trunc64("ReplaceResponse", response))
	return response, nil
}

func (s *multiMapServer) Contains(ctx context.Context, request *multimapv1.ContainsRequest) (*multimapv1.ContainsResponse, error) {
	log.Debugw("Contains",
		logging.Trunc64("ContainsRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Contains",
			logging.Trunc64("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Contains(ctx, request)
	if err != nil {
		log.Debugw("Contains",
			logging.Trunc64("ContainsRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Contains",
		logging.Trunc64("ContainsResponse", response))
	return response, nil
}

func (s *multiMapServer) Get(ctx context.Context, request *multimapv1.GetRequest) (*multimapv1.GetResponse, error) {
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

func (s *multiMapServer) Remove(ctx context.Context, request *multimapv1.RemoveRequest) (*multimapv1.RemoveResponse, error) {
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

func (s *multiMapServer) RemoveAll(ctx context.Context, request *multimapv1.RemoveAllRequest) (*multimapv1.RemoveAllResponse, error) {
	log.Debugw("RemoveAll",
		logging.Trunc64("RemoveAllRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("RemoveAll",
			logging.Trunc64("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.RemoveAll(ctx, request)
	if err != nil {
		log.Debugw("RemoveAll",
			logging.Trunc64("RemoveAllRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("RemoveAll",
		logging.Trunc64("RemoveAllResponse", response))
	return response, nil
}

func (s *multiMapServer) RemoveEntries(ctx context.Context, request *multimapv1.RemoveEntriesRequest) (*multimapv1.RemoveEntriesResponse, error) {
	log.Debugw("RemoveEntries",
		logging.Trunc64("RemoveEntriesRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("RemoveEntries",
			logging.Trunc64("RemoveEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.RemoveEntries(ctx, request)
	if err != nil {
		log.Debugw("RemoveEntries",
			logging.Trunc64("RemoveEntriesRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("RemoveEntries",
		logging.Trunc64("RemoveEntriesResponse", response))
	return response, nil
}

func (s *multiMapServer) Clear(ctx context.Context, request *multimapv1.ClearRequest) (*multimapv1.ClearResponse, error) {
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

func (s *multiMapServer) Events(request *multimapv1.EventsRequest, server multimapv1.MultiMap_EventsServer) error {
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

func (s *multiMapServer) Entries(request *multimapv1.EntriesRequest, server multimapv1.MultiMap_EntriesServer) error {
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

var _ multimapv1.MultiMapServer = (*multiMapServer)(nil)
