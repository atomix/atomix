// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newCounterServer(proxies *primitive.ProxyManager[Counter]) v1.CounterServer {
	return &counterServer{
		proxies: proxies,
	}
}

type counterServer struct {
	proxies *primitive.ProxyManager[Counter]
}

func (s *counterServer) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Set(ctx, request)
}

func (s *counterServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Get(ctx, request)
}

func (s *counterServer) Increment(ctx context.Context, request *v1.IncrementRequest) (*v1.IncrementResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Increment(ctx, request)
}

func (s *counterServer) Decrement(ctx context.Context, request *v1.DecrementRequest) (*v1.DecrementResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Decrement(ctx, request)
}

var _ v1.CounterServer = (*counterServer)(nil)
