// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	countermapv1 "github.com/atomix/atomix/api/runtime/countermap/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
)

var log = logging.GetLogger()

const truncLen = 250

func newCounterMapServer(client *runtime.PrimitiveClient[CounterMap]) countermapv1.CounterMapServer {
	return &counterMapServer{
		client: client,
	}
}

type counterMapServer struct {
	client *runtime.PrimitiveClient[CounterMap]
}

func (s *counterMapServer) Create(ctx context.Context, request *countermapv1.CreateRequest) (*countermapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	client, err := s.client.Create(ctx, request.ID, request.Tags)
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

func (s *counterMapServer) Close(ctx context.Context, request *countermapv1.CloseRequest) (*countermapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Size(ctx context.Context, request *countermapv1.SizeRequest) (*countermapv1.SizeResponse, error) {
	log.Debugw("Size",
		logging.Trunc64("SizeRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Set(ctx context.Context, request *countermapv1.SetRequest) (*countermapv1.SetResponse, error) {
	log.Debugw("Set",
		logging.Trunc64("SetRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
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

func (s *counterMapServer) Increment(ctx context.Context, request *countermapv1.IncrementRequest) (*countermapv1.IncrementResponse, error) {
	log.Debugw("Increment",
		logging.Trunc64("IncrementRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Increment",
			logging.Trunc64("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Increment(ctx, request)
	if err != nil {
		log.Debugw("Increment",
			logging.Trunc64("IncrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Increment",
		logging.Trunc64("IncrementResponse", response))
	return response, nil
}

func (s *counterMapServer) Decrement(ctx context.Context, request *countermapv1.DecrementRequest) (*countermapv1.DecrementResponse, error) {
	log.Debugw("Decrement",
		logging.Trunc64("DecrementRequest", request))
	client, err := s.client.Get(request.ID)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Decrement",
			logging.Trunc64("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Decrement(ctx, request)
	if err != nil {
		log.Debugw("Decrement",
			logging.Trunc64("DecrementRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Decrement",
		logging.Trunc64("DecrementResponse", response))
	return response, nil
}

func (s *counterMapServer) Insert(ctx context.Context, request *countermapv1.InsertRequest) (*countermapv1.InsertResponse, error) {
	log.Debugw("Insert",
		logging.Trunc64("InsertRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Update(ctx context.Context, request *countermapv1.UpdateRequest) (*countermapv1.UpdateResponse, error) {
	log.Debugw("Update",
		logging.Trunc64("UpdateRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Get(ctx context.Context, request *countermapv1.GetRequest) (*countermapv1.GetResponse, error) {
	log.Debugw("Get",
		logging.Trunc64("GetRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Remove(ctx context.Context, request *countermapv1.RemoveRequest) (*countermapv1.RemoveResponse, error) {
	log.Debugw("Remove",
		logging.Trunc64("RemoveRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Clear(ctx context.Context, request *countermapv1.ClearRequest) (*countermapv1.ClearResponse, error) {
	log.Debugw("Clear",
		logging.Trunc64("ClearRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Lock(ctx context.Context, request *countermapv1.LockRequest) (*countermapv1.LockResponse, error) {
	log.Debugw("Lock",
		logging.Trunc64("LockRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Unlock(ctx context.Context, request *countermapv1.UnlockRequest) (*countermapv1.UnlockResponse, error) {
	log.Debugw("Unlock",
		logging.Trunc64("UnlockRequest", request))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Events(request *countermapv1.EventsRequest, server countermapv1.CounterMap_EventsServer) error {
	log.Debugw("Events",
		logging.Trunc64("EventsRequest", request),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
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

func (s *counterMapServer) Entries(request *countermapv1.EntriesRequest, server countermapv1.CounterMap_EntriesServer) error {
	log.Debugw("Entries",
		logging.Trunc64("EntriesRequest", request),
		logging.String("State", "started"))
	client, err := s.client.Get(request.ID)
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

var _ countermapv1.CounterMapServer = (*counterMapServer)(nil)
