// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/errors"
)

func newValueV1Server(proxies *atom.Registry[Value]) v1.ValueServer {
	return &valueV1Server{
		proxies: proxies,
	}
}

type valueV1Server struct {
	proxies *atom.Registry[Value]
}

func (s *valueV1Server) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Set(ctx, request)
}

func (s *valueV1Server) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Get(ctx, request)
}

func (s *valueV1Server) Events(request *v1.EventsRequest, server v1.Value_EventsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Events(request, server)
}

var _ v1.ValueServer = (*valueV1Server)(nil)
