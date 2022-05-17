// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/errors"
)

func newSetV1Server(proxies *atom.Registry[Set]) v1.SetServer {
	return &setV1Server{
		proxies: proxies,
	}
}

type setV1Server struct {
	proxies *atom.Registry[Set]
}

func (s *setV1Server) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Size(ctx, request)
}

func (s *setV1Server) Contains(ctx context.Context, request *v1.ContainsRequest) (*v1.ContainsResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Contains(ctx, request)
}

func (s *setV1Server) Add(ctx context.Context, request *v1.AddRequest) (*v1.AddResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Add(ctx, request)
}

func (s *setV1Server) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Remove(ctx, request)
}

func (s *setV1Server) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Clear(ctx, request)
}

func (s *setV1Server) Events(request *v1.EventsRequest, server v1.Set_EventsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Events(request, server)
}

func (s *setV1Server) Elements(request *v1.ElementsRequest, server v1.Set_ElementsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.Atom)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.Atom))
	}
	return proxy.Elements(request, server)
}

var _ v1.SetServer = (*setV1Server)(nil)
