// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	valuev1 "github.com/atomix/atomix/api/runtime/value/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type ValueProxy interface {
	runtime.PrimitiveProxy
	// Set sets the value
	Set(context.Context, *valuev1.SetRequest) (*valuev1.SetResponse, error)
	// Insert inserts the value
	Insert(context.Context, *valuev1.InsertRequest) (*valuev1.InsertResponse, error)
	// Update updates the value
	Update(context.Context, *valuev1.UpdateRequest) (*valuev1.UpdateResponse, error)
	// Get gets the value
	Get(context.Context, *valuev1.GetRequest) (*valuev1.GetResponse, error)
	// Delete deletes the value
	Delete(context.Context, *valuev1.DeleteRequest) (*valuev1.DeleteResponse, error)
	// Watch watches the value
	Watch(*valuev1.WatchRequest, valuev1.Value_WatchServer) error
	// Events watches for value change events
	Events(*valuev1.EventsRequest, valuev1.Value_EventsServer) error
}

func NewValueServer(rt *runtime.Runtime) valuev1.ValueServer {
	return &valueServer{
		primitives: runtime.NewPrimitiveRegistry[ValueProxy](valuev1.PrimitiveType, rt),
	}
}

type valueServer struct {
	valuev1.ValuesServer
	primitives runtime.PrimitiveRegistry[ValueProxy]
}

func (s *valueServer) Set(ctx context.Context, request *valuev1.SetRequest) (*valuev1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc64("SetRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Set",
			logging.Trunc64("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Set(ctx, request)
	if err != nil {
		log.Debugw("Set",
			logging.Trunc64("SetRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Set",
		logging.Trunc64("SetResponse", response))
	return response, nil
}

func (s *valueServer) Insert(ctx context.Context, request *valuev1.InsertRequest) (*valuev1.InsertResponse, error) {
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

func (s *valueServer) Update(ctx context.Context, request *valuev1.UpdateRequest) (*valuev1.UpdateResponse, error) {
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

func (s *valueServer) Get(ctx context.Context, request *valuev1.GetRequest) (*valuev1.GetResponse, error) {
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

func (s *valueServer) Delete(ctx context.Context, request *valuev1.DeleteRequest) (*valuev1.DeleteResponse, error) {
	log.Debugw("Delete",
		logging.Trunc64("DeleteRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Delete",
			logging.Trunc64("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Delete(ctx, request)
	if err != nil {
		log.Debugw("Delete",
			logging.Trunc64("DeleteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Delete",
		logging.Trunc64("DeleteResponse", response))
	return response, nil
}

func (s *valueServer) Watch(request *valuev1.WatchRequest, server valuev1.Value_WatchServer) error {
	log.Debugw("Watch",
		logging.Trunc64("WatchRequest", request),
		logging.String("State", "started"))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Watch",
			logging.Trunc64("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Watch(request, server)
	if err != nil {
		log.Debugw("Watch",
			logging.Trunc64("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

func (s *valueServer) Events(request *valuev1.EventsRequest, server valuev1.Value_EventsServer) error {
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

var _ valuev1.ValueServer = (*valueServer)(nil)
