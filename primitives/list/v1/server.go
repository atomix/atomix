// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newListServer(proxies *primitive.ProxyManager[List]) v1.ListServer {
	return &listServer{
		proxies: proxies,
	}
}

type listServer struct {
	proxies *primitive.ProxyManager[List]
}

func (s *listServer) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Size(ctx, request)
}

func (s *listServer) Append(ctx context.Context, request *v1.AppendRequest) (*v1.AppendResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Append(ctx, request)
}

func (s *listServer) Insert(ctx context.Context, request *v1.InsertRequest) (*v1.InsertResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Insert(ctx, request)
}

func (s *listServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Get(ctx, request)
}

func (s *listServer) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Set(ctx, request)
}

func (s *listServer) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Remove(ctx, request)
}

func (s *listServer) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Clear(ctx, request)
}

func (s *listServer) Events(request *v1.EventsRequest, server v1.List_EventsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

func (s *listServer) Elements(request *v1.ElementsRequest, server v1.List_ElementsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Elements(request, server)
}

var _ v1.ListServer = (*listServer)(nil)
