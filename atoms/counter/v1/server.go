// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/errors"
)

func newCounterV1Server(proxies *atom.Registry[Counter]) v1.CounterServer {
	return &counterV1Server{
		proxies: proxies,
	}
}

type counterV1Server struct {
	proxies *atom.Registry[Counter]
}

func (s *counterV1Server) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Set(ctx, request)
}

func (s *counterV1Server) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Get(ctx, request)
}

func (s *counterV1Server) Increment(ctx context.Context, request *v1.IncrementRequest) (*v1.IncrementResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Increment(ctx, request)
}

func (s *counterV1Server) Decrement(ctx context.Context, request *v1.DecrementRequest) (*v1.DecrementResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Decrement(ctx, request)
}

var _ v1.CounterServer = (*counterV1Server)(nil)
