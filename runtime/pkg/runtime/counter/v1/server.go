// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/atomix/api/pkg/runtime/counter/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/runtime"
)

var log = logging.GetLogger()

const truncLen = 250

func newCounterServer(client *runtime.PrimitiveClient[Counter]) counterv1.CounterServer {
	return &counterServer{
		client: client,
	}
}

type counterServer struct {
	client *runtime.PrimitiveClient[Counter]
}

func (s *counterServer) Create(ctx context.Context, request *counterv1.CreateRequest) (*counterv1.CreateResponse, error) {
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

func (s *counterServer) Close(ctx context.Context, request *counterv1.CloseRequest) (*counterv1.CloseResponse, error) {
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

func (s *counterServer) Set(ctx context.Context, request *counterv1.SetRequest) (*counterv1.SetResponse, error) {
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

func (s *counterServer) Update(ctx context.Context, request *counterv1.UpdateRequest) (*counterv1.UpdateResponse, error) {
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

func (s *counterServer) Get(ctx context.Context, request *counterv1.GetRequest) (*counterv1.GetResponse, error) {
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

func (s *counterServer) Increment(ctx context.Context, request *counterv1.IncrementRequest) (*counterv1.IncrementResponse, error) {
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

func (s *counterServer) Decrement(ctx context.Context, request *counterv1.DecrementRequest) (*counterv1.DecrementResponse, error) {
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

var _ counterv1.CounterServer = (*counterServer)(nil)
