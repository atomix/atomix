// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newListV1Server(proxies *primitive.Registry[List]) v1.ListServer {
	return &listV1Server{
		proxies: proxies,
	}
}

type listV1Server struct {
	proxies *primitive.Registry[List]
}

func (s *listV1Server) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Size(ctx, request)
}

func (s *listV1Server) Append(ctx context.Context, request *v1.AppendRequest) (*v1.AppendResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Append(ctx, request)
}

func (s *listV1Server) Insert(ctx context.Context, request *v1.InsertRequest) (*v1.InsertResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Insert(ctx, request)
}

func (s *listV1Server) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Get(ctx, request)
}

func (s *listV1Server) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Set(ctx, request)
}

func (s *listV1Server) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Remove(ctx, request)
}

func (s *listV1Server) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Clear(ctx, request)
}

func (s *listV1Server) Events(request *v1.EventsRequest, server v1.List_EventsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Events(request, server)
}

func (s *listV1Server) Elements(request *v1.ElementsRequest, server v1.List_ElementsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Elements(request, server)
}

var _ v1.ListServer = (*listV1Server)(nil)
