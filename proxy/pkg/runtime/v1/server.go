// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
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

func (s *runtimeServer) Connect(ctx context.Context, request *runtimev1.ConnectRequest) (*runtimev1.ConnectResponse, error) {
	log.Debugw("Connect",
		logging.Stringer("ConnectRequest", request))
	if err := s.runtime.Connect(ctx, request.DriverID, request.Store); err != nil {
		err = errors.ToProto(err)
		log.Debugw("Connect",
			logging.Stringer("ConnectRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.ConnectResponse{}
	log.Debugw("Connect",
		logging.Stringer("ConnectRequest", request),
		logging.Stringer("ConnectResponse", response))
	return response, nil
}

func (s *runtimeServer) Configure(ctx context.Context, request *runtimev1.ConfigureRequest) (*runtimev1.ConfigureResponse, error) {
	log.Debugw("Configure",
		logging.Stringer("ConfigureRequest", request))
	if err := s.runtime.Configure(ctx, request.Store); err != nil {
		err = errors.ToProto(err)
		log.Debugw("Configure",
			logging.Stringer("ConfigureRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.ConfigureResponse{}
	log.Debugw("Configure",
		logging.Stringer("ConfigureRequest", request),
		logging.Stringer("ConfigureResponse", response))
	return response, nil
}

func (s *runtimeServer) Disconnect(ctx context.Context, request *runtimev1.DisconnectRequest) (*runtimev1.DisconnectResponse, error) {
	log.Debugw("Disconnect",
		logging.Stringer("DisconnectRequest", request))
	if err := s.runtime.Disconnect(ctx, request.StoreID); err != nil {
		err = errors.ToProto(err)
		log.Debugw("Disconnect",
			logging.Stringer("DisconnectRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &runtimev1.DisconnectResponse{}
	log.Debugw("Disconnect",
		logging.Stringer("DisconnectRequest", request),
		logging.Stringer("DisconnectResponse", response))
	return response, nil
}
