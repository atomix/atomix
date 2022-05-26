// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
)

func NewServer(proxies Manager) runtimev1.ProxyServiceServer {
	return &proxyServiceServer{
		proxies: proxies,
	}
}

type proxyServiceServer struct {
	proxies Manager
}

func (s *proxyServiceServer) GetProxy(ctx context.Context, request *runtimev1.GetProxyRequest) (*runtimev1.GetProxyResponse, error) {
	log.Debugw("GetProxy",
		logging.Stringer("GetProxyRequest", request))

	proxy, ok := s.proxies.Store().Get(&request.ProxyID)
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

	proxies := s.proxies.Store().List()
	response := &runtimev1.ListProxiesResponse{
		Proxies: proxies,
	}
	log.Debugw("ListProxies",
		logging.Stringer("ListProxiesResponse", response))
	return response, nil
}

var _ runtimev1.ProxyServiceServer = (*proxyServiceServer)(nil)
