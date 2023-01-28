// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	countermapv1 "github.com/atomix/atomix/api/runtime/countermap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type CounterMapProvider interface {
	NewCounterMapV1(ctx context.Context, id runtimev1.PrimitiveID) (CounterMapProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (CounterMapProxy, bool, error) {
	if provider, ok := conn.(CounterMapProvider); ok {
		counterMap, err := provider.NewCounterMapV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return counterMap, true, nil
	}
	return nil, false, nil
}

func NewCounterMapsServer(rt *runtime.Runtime) countermapv1.CounterMapsServer {
	return &counterMapsServer{
		manager: runtime.NewPrimitiveManager[CounterMapProxy, *countermapv1.CounterMapConfig](countermapv1.PrimitiveType, resolve, rt),
	}
}

type counterMapsServer struct {
	manager runtime.PrimitiveManager[*countermapv1.CounterMapConfig]
}

func (s *counterMapsServer) Create(ctx context.Context, request *countermapv1.CreateRequest) (*countermapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *counterMapsServer) Close(ctx context.Context, request *countermapv1.CloseRequest) (*countermapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &countermapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ countermapv1.CounterMapsServer = (*counterMapsServer)(nil)
