// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/runtime/store"
)

func newProxyServiceServer(store store.Store[*runtimev1.ProxyId, *runtimev1.Proxy]) runtimev1.ProxyServiceServer {
	return &proxyServiceServer{
		store: store,
	}
}

type proxyServiceServer struct {
	store store.Store[*runtimev1.ProxyId, *runtimev1.Proxy]
}

func (s *proxyServiceServer) GetProxy(ctx context.Context, request *runtimev1.GetProxyRequest) (*runtimev1.GetProxyResponse, error) {
	log.Debugw("GetProxy",
		logging.Stringer("GetProxyRequest", request))

	proxy, ok := s.store.Get(&request.ProxyID)
	if !ok {
		err := errors.NewNotFound("proxy '%s' not found", request.ProxyID)
		log.Warnw("GetProxy",
			logging.Stringer("GetProxyRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.GetProxyResponse{
		Proxy: proxy,
	}
	log.Debugw("GetProxy",
		logging.Stringer("GetProxyResponse", response))
	return response, nil
}

func (s *proxyServiceServer) ListProxies(ctx context.Context, request *runtimev1.ListProxiesRequest) (*runtimev1.ListProxiesResponse, error) {
	log.Debugw("ListProxies",
		logging.Stringer("ListProxiesRequest", request))

	proxies := s.store.List()
	response := &runtimev1.ListProxiesResponse{
		Proxies: proxies,
	}
	log.Debugw("ListProxies",
		logging.Stringer("ListProxiesResponse", response))
	return response, nil
}

var _ runtimev1.ProxyServiceServer = (*proxyServiceServer)(nil)
