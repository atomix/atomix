// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type MapProvider interface {
	NewMapV1(ctx context.Context, id runtimev1.PrimitiveID) (MapProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (MapProxy, bool, error) {
	if provider, ok := conn.(MapProvider); ok {
		_map, err := provider.NewMapV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return _map, true, nil
	}
	return nil, false, nil
}

func NewMapsServer(rt *runtime.Runtime) mapv1.MapsServer {
	return &mapsServer{
		manager: runtime.NewPrimitiveManager[MapProxy, *mapv1.MapConfig](mapv1.PrimitiveType, resolve, rt),
	}
}

type mapsServer struct {
	manager runtime.PrimitiveManager[*mapv1.MapConfig]
}

func (s *mapsServer) Create(ctx context.Context, request *mapv1.CreateRequest) (*mapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *mapsServer) Close(ctx context.Context, request *mapv1.CloseRequest) (*mapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &mapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ mapv1.MapsServer = (*mapsServer)(nil)
