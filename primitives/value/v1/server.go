// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newValueServer(proxies *primitive.ProxyManager[Value]) v1.ValueServer {
	return &valueServer{
		proxies: proxies,
	}
}

type valueServer struct {
	proxies *primitive.ProxyManager[Value]
}

func (s *valueServer) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Set(ctx, request)
}

func (s *valueServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Get(ctx, request)
}

func (s *valueServer) Events(request *v1.EventsRequest, server v1.Value_EventsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

var _ v1.ValueServer = (*valueServer)(nil)
