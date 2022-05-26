// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newMapServer(proxies *primitive.ProxyManager[Map]) mapv1.MapServer {
	return &mapServer{
		proxies: proxies,
	}
}

type mapServer struct {
	proxies *primitive.ProxyManager[Map]
}

func (s *mapServer) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Size(ctx, request)
}

func (s *mapServer) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Put(ctx, request)
}

func (s *mapServer) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Get(ctx, request)
}

func (s *mapServer) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Remove(ctx, request)
}

func (s *mapServer) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Clear(ctx, request)
}

func (s *mapServer) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

func (s *mapServer) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Entries(request, server)
}

var _ mapv1.MapServer = (*mapServer)(nil)
