// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newIndexedMapV1Server(proxies *primitive.Registry[IndexedMap]) v1.IndexedMapServer {
	return &indexedMapV1Server{
		proxies: proxies,
	}
}

type indexedMapV1Server struct {
	proxies *primitive.Registry[IndexedMap]
}

func (s *indexedMapV1Server) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Size(ctx, request)
}

func (s *indexedMapV1Server) Put(ctx context.Context, request *v1.PutRequest) (*v1.PutResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Put(ctx, request)
}

func (s *indexedMapV1Server) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Get(ctx, request)
}

func (s *indexedMapV1Server) FirstEntry(ctx context.Context, request *v1.FirstEntryRequest) (*v1.FirstEntryResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.FirstEntry(ctx, request)
}

func (s *indexedMapV1Server) LastEntry(ctx context.Context, request *v1.LastEntryRequest) (*v1.LastEntryResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.LastEntry(ctx, request)
}

func (s *indexedMapV1Server) PrevEntry(ctx context.Context, request *v1.PrevEntryRequest) (*v1.PrevEntryResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.PrevEntry(ctx, request)
}

func (s *indexedMapV1Server) NextEntry(ctx context.Context, request *v1.NextEntryRequest) (*v1.NextEntryResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.NextEntry(ctx, request)
}

func (s *indexedMapV1Server) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Remove(ctx, request)
}

func (s *indexedMapV1Server) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return nil, errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Clear(ctx, request)
}

func (s *indexedMapV1Server) Events(request *v1.EventsRequest, server v1.IndexedMap_EventsServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Events(request, server)
}

func (s *indexedMapV1Server) Entries(request *v1.EntriesRequest, server v1.IndexedMap_EntriesServer) error {
	proxy, ok := s.proxies.GetProxy(request.Headers.PrimitiveID)
	if !ok {
		return errors.ToProto(errors.NewForbidden("proxy '%s' not open", request.Headers.PrimitiveID))
	}
	return proxy.Entries(request, server)
}

var _ v1.IndexedMapServer = (*indexedMapV1Server)(nil)
