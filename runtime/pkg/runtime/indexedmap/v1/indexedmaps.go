// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/atomix/api/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type IndexedMapProvider interface {
	NewIndexedMapV1(ctx context.Context, id runtimev1.PrimitiveID) (IndexedMapProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (IndexedMapProxy, bool, error) {
	if provider, ok := conn.(IndexedMapProvider); ok {
		indexedMap, err := provider.NewIndexedMapV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return indexedMap, true, nil
	}
	return nil, false, nil
}

func NewIndexedMapsServer(rt *runtime.Runtime) indexedmapv1.IndexedMapsServer {
	return &indexedMapsServer{
		manager: runtime.NewPrimitiveManager[IndexedMapProxy, *indexedmapv1.IndexedMapConfig](indexedmapv1.PrimitiveType, resolve, rt),
	}
}

type indexedMapsServer struct {
	manager runtime.PrimitiveManager[*indexedmapv1.IndexedMapConfig]
}

func (s *indexedMapsServer) Create(ctx context.Context, request *indexedmapv1.CreateRequest) (*indexedmapv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *indexedMapsServer) Close(ctx context.Context, request *indexedmapv1.CloseRequest) (*indexedmapv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &indexedmapv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ indexedmapv1.IndexedMapsServer = (*indexedMapsServer)(nil)
