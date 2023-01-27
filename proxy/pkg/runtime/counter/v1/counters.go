// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	runtime "github.com/atomix/atomix/proxy/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
)

type CounterProvider interface {
	NewCounterV1(ctx context.Context, id runtimev1.PrimitiveID) (CounterProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (CounterProxy, bool, error) {
	if provider, ok := conn.(CounterProvider); ok {
		counter, err := provider.NewCounterV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return counter, true, nil
	}
	return nil, false, nil
}

func NewCountersServer(rt *runtime.Runtime) counterv1.CountersServer {
	return &countersServer{
		manager: runtime.NewPrimitiveManager[CounterProxy, *counterv1.CounterConfig](counterv1.PrimitiveType, resolve, rt),
	}
}

type countersServer struct {
	manager runtime.PrimitiveManager[*counterv1.CounterConfig]
}

func (s *countersServer) Create(ctx context.Context, request *counterv1.CreateRequest) (*counterv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *countersServer) Close(ctx context.Context, request *counterv1.CloseRequest) (*counterv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &counterv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ counterv1.CountersServer = (*countersServer)(nil)
