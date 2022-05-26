// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newSetServer(proxies *primitive.ProxyManager[Set]) v1.SetServer {
	return &setServer{
		proxies: proxies,
	}
}

type setServer struct {
	proxies *primitive.ProxyManager[Set]
}

func (s *setServer) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Size(ctx, request)
}

func (s *setServer) Contains(ctx context.Context, request *v1.ContainsRequest) (*v1.ContainsResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Contains(ctx, request)
}

func (s *setServer) Add(ctx context.Context, request *v1.AddRequest) (*v1.AddResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Add(ctx, request)
}

func (s *setServer) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Remove(ctx, request)
}

func (s *setServer) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Clear(ctx, request)
}

func (s *setServer) Events(request *v1.EventsRequest, server v1.Set_EventsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

func (s *setServer) Elements(request *v1.ElementsRequest, server v1.Set_ElementsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Elements(request, server)
}

var _ v1.SetServer = (*setServer)(nil)
