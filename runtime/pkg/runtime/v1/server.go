// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
)

func NewRuntimeServer(runtime *Runtime) runtimev1.RuntimeServer {
	return &runtimeServer{
		runtime: runtime,
	}
}

type runtimeServer struct {
	runtime *Runtime
}

func (s *runtimeServer) AddRoute(ctx context.Context, request *runtimev1.AddRouteRequest) (*runtimev1.AddRouteResponse, error) {
	log.Debugw("AddRoute",
		logging.Stringer("AddRouteRequest", request))
	if err := s.runtime.AddRoute(ctx, request.Route); err != nil {
		log.Debugw("AddRoute",
			logging.Stringer("AddRouteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.AddRouteResponse{}
	log.Debugw("AddRoute",
		logging.Stringer("AddRouteRequest", request),
		logging.Stringer("AddRouteResponse", response))
	return response, nil
}

func (s *runtimeServer) RemoveRoute(ctx context.Context, request *runtimev1.RemoveRouteRequest) (*runtimev1.RemoveRouteResponse, error) {
	log.Debugw("RemoveRoute",
		logging.Stringer("RemoveRouteRequest", request))
	if err := s.runtime.RemoveRoute(ctx, request.StoreID); err != nil {
		log.Debugw("RemoveRoute",
			logging.Stringer("RemoveRouteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.RemoveRouteResponse{}
	log.Debugw("RemoveRoute",
		logging.Stringer("RemoveRouteRequest", request),
		logging.Stringer("RemoveRouteResponse", response))
	return response, nil
}

func (s *runtimeServer) ConnectStore(ctx context.Context, request *runtimev1.ConnectStoreRequest) (*runtimev1.ConnectStoreResponse, error) {
	log.Debugw("ConnectStore",
		logging.Stringer("ConnectStoreRequest", request))
	if err := s.runtime.ConnectStore(ctx, request.DriverID, request.Store); err != nil {
		log.Debugw("ConnectStore",
			logging.Stringer("ConnectStoreRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.ConnectStoreResponse{}
	log.Debugw("ConnectStore",
		logging.Stringer("ConnectStoreRequest", request),
		logging.Stringer("ConnectStoreResponse", response))
	return response, nil
}

func (s *runtimeServer) ConfigureStore(ctx context.Context, request *runtimev1.ConfigureStoreRequest) (*runtimev1.ConfigureStoreResponse, error) {
	log.Debugw("ConfigureStore",
		logging.Stringer("ConfigureStoreRequest", request))
	if err := s.runtime.ConfigureStore(ctx, request.Store); err != nil {
		log.Debugw("ConfigureStore",
			logging.Stringer("ConfigureStoreRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.ConfigureStoreResponse{}
	log.Debugw("ConfigureStore",
		logging.Stringer("ConfigureStoreRequest", request),
		logging.Stringer("ConfigureStoreResponse", response))
	return response, nil
}

func (s *runtimeServer) DisconnectStore(ctx context.Context, request *runtimev1.DisconnectStoreRequest) (*runtimev1.DisconnectStoreResponse, error) {
	log.Debugw("DisconnectStore",
		logging.Stringer("DisconnectStoreRequest", request))
	if err := s.runtime.DisconnectStore(ctx, request.StoreID); err != nil {
		log.Debugw("DisconnectStore",
			logging.Stringer("DisconnectStoreRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.DisconnectStoreResponse{}
	log.Debugw("DisconnectStore",
		logging.Stringer("DisconnectStoreRequest", request),
		logging.Stringer("DisconnectStoreResponse", response))
	return response, nil
}
