// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/primitive/map/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newProxyingMap(proxies *primitive.Manager[Map]) v1.MapServer {
	return &proxyingMap{
		proxies: proxies,
	}
}

type proxyingMap struct {
	proxies *primitive.Manager[Map]
}

func (s *proxyingMap) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Size(ctx, request)
}

func (s *proxyingMap) Put(ctx context.Context, request *v1.PutRequest) (*v1.PutResponse, error) {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Put(ctx, request)
}

func (s *proxyingMap) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Get(ctx, request)
}

func (s *proxyingMap) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Remove(ctx, request)
}

func (s *proxyingMap) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Clear(ctx, request)
}

func (s *proxyingMap) Events(request *v1.EventsRequest, server v1.Map_EventsServer) error {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

func (s *proxyingMap) Entries(request *v1.EntriesRequest, server v1.Map_EntriesServer) error {
	proxy, err := s.proxies.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Entries(request, server)
}

var _ v1.MapServer = (*proxyingMap)(nil)
