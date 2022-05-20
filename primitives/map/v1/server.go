// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newMapV1Server(proxies *primitive.Registry[Map]) v1.MapServer {
	return &mapV1Server{
		proxies: proxies,
	}
}

type mapV1Server struct {
	proxies *primitive.Registry[Map]
}

func (s *mapV1Server) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Size(ctx, request)
}

func (s *mapV1Server) Put(ctx context.Context, request *v1.PutRequest) (*v1.PutResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Put(ctx, request)
}

func (s *mapV1Server) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Get(ctx, request)
}

func (s *mapV1Server) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Remove(ctx, request)
}

func (s *mapV1Server) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Clear(ctx, request)
}

func (s *mapV1Server) Events(request *v1.EventsRequest, server v1.Map_EventsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Events(request, server)
}

func (s *mapV1Server) Entries(request *v1.EntriesRequest, server v1.Map_EntriesServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Entries(request, server)
}

var _ v1.MapServer = (*mapV1Server)(nil)
