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

func (s *runtimeServer) ConnectRoute(ctx context.Context, request *runtimev1.ConnectRouteRequest) (*runtimev1.ConnectRouteResponse, error) {
	log.Debugw("ConnectRoute",
		logging.Stringer("ConnectRouteRequest", request))
	if err := s.runtime.ConnectRoute(ctx, request.DriverID, request.Route); err != nil {
		log.Debugw("ConnectRoute",
			logging.Stringer("ConnectRouteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.ConnectRouteResponse{}
	log.Debugw("ConnectRoute",
		logging.Stringer("ConnectRouteRequest", request),
		logging.Stringer("ConnectRouteResponse", response))
	return response, nil
}

func (s *runtimeServer) ConfigureRoute(ctx context.Context, request *runtimev1.ConfigureRouteRequest) (*runtimev1.ConfigureRouteResponse, error) {
	log.Debugw("ConfigureRoute",
		logging.Stringer("ConfigureRouteRequest", request))
	if err := s.runtime.ConfigureRoute(ctx, request.Route); err != nil {
		log.Debugw("ConfigureRoute",
			logging.Stringer("ConfigureRouteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.ConfigureRouteResponse{}
	log.Debugw("ConfigureRoute",
		logging.Stringer("ConfigureRouteRequest", request),
		logging.Stringer("ConfigureRouteResponse", response))
	return response, nil
}

func (s *runtimeServer) DisconnectRoute(ctx context.Context, request *runtimev1.DisconnectRouteRequest) (*runtimev1.DisconnectRouteResponse, error) {
	log.Debugw("DisconnectRoute",
		logging.Stringer("DisconnectRouteRequest", request))
	if err := s.runtime.DisconnectRoute(ctx, request.RouteID); err != nil {
		log.Debugw("DisconnectRoute",
			logging.Stringer("DisconnectRouteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &runtimev1.DisconnectRouteResponse{}
	log.Debugw("DisconnectRoute",
		logging.Stringer("DisconnectRouteRequest", request),
		logging.Stringer("DisconnectRouteResponse", response))
	return response, nil
}
