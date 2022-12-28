// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

const (
	Name       = "Map"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func NewMapServer(rt *runtime.Runtime) mapv1.MapServer {
	return &mapServer{
		manager: runtime.NewPrimitiveManager[mapv1.MapServer](PrimitiveType, rt),
	}
}

type mapServer struct {
	manager *runtime.PrimitiveManager[mapv1.MapServer]
}

func (s *mapServer) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	client, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Debugw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *mapServer) Close(ctx context.Context, request *mapv1.CloseRequest) (*mapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Debugw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

func (s *mapServer) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc64("SizeRequest", request))
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
	client, err := s.manager.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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
