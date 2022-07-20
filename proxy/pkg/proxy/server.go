// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	proxyv1 "github.com/atomix/runtime/api/atomix/proxy/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/runtime"
)

func newProxyServer(runtime *Runtime) proxyv1.ProxyServer {
	return &proxyServer{
		runtime: runtime,
	}
}

type proxyServer struct {
	runtime *Runtime
}

func (s *proxyServer) Connect(ctx context.Context, request *proxyv1.ConnectRequest) (*proxyv1.ConnectResponse, error) {
	log.Debugw("Connect",
		logging.Stringer("ConnectRequest", request))
	storeID := StoreID{
		Namespace: request.StoreID.Namespace,
		Name:      request.StoreID.Name,
	}
	driverID := runtime.DriverID{
		Name:    request.DriverID.Name,
		Version: request.DriverID.Version,
	}
	if err := s.runtime.connect(ctx, storeID, driverID, request.Config); err != nil {
		err = errors.ToProto(err)
		log.Debugw("Connect",
			logging.Stringer("ConnectRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &proxyv1.ConnectResponse{}
	log.Debugw("Connect",
		logging.Stringer("ConnectRequest", request),
		logging.Stringer("ConnectResponse", response))
	return response, nil
}

func (s *proxyServer) Configure(ctx context.Context, request *proxyv1.ConfigureRequest) (*proxyv1.ConfigureResponse, error) {
	log.Debugw("Configure",
		logging.Stringer("ConfigureRequest", request))
	storeID := StoreID{
		Namespace: request.StoreID.Namespace,
		Name:      request.StoreID.Name,
	}
	if err := s.runtime.configure(ctx, storeID, request.Config); err != nil {
		err = errors.ToProto(err)
		log.Debugw("Configure",
			logging.Stringer("ConfigureRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &proxyv1.ConfigureResponse{}
	log.Debugw("Configure",
		logging.Stringer("ConfigureRequest", request),
		logging.Stringer("ConfigureResponse", response))
	return response, nil
}

func (s *proxyServer) Disconnect(ctx context.Context, request *proxyv1.DisconnectRequest) (*proxyv1.DisconnectResponse, error) {
	log.Debugw("Disconnect",
		logging.Stringer("DisconnectRequest", request))
	storeID := StoreID{
		Namespace: request.StoreID.Namespace,
		Name:      request.StoreID.Name,
	}
	if err := s.runtime.disconnect(ctx, storeID); err != nil {
		err = errors.ToProto(err)
		log.Debugw("Disconnect",
			logging.Stringer("DisconnectRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}
	response := &proxyv1.DisconnectResponse{}
	log.Debugw("Disconnect",
		logging.Stringer("DisconnectRequest", request),
		logging.Stringer("DisconnectResponse", response))
	return response, nil
}
