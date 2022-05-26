// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newIndexedMapServer(proxies *primitive.ProxyManager[IndexedMap]) v1.IndexedMapServer {
	return &indexedMapServer{
		proxies: proxies,
	}
}

type indexedMapServer struct {
	proxies *primitive.ProxyManager[IndexedMap]
}

func (s *indexedMapServer) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Size(ctx, request)
}

func (s *indexedMapServer) Put(ctx context.Context, request *v1.PutRequest) (*v1.PutResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Put(ctx, request)
}

func (s *indexedMapServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Get(ctx, request)
}

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *v1.FirstEntryRequest) (*v1.FirstEntryResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.FirstEntry(ctx, request)
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *v1.LastEntryRequest) (*v1.LastEntryResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.LastEntry(ctx, request)
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *v1.PrevEntryRequest) (*v1.PrevEntryResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.PrevEntry(ctx, request)
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *v1.NextEntryRequest) (*v1.NextEntryResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.NextEntry(ctx, request)
}

func (s *indexedMapServer) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Remove(ctx, request)
}

func (s *indexedMapServer) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Clear(ctx, request)
}

func (s *indexedMapServer) Events(request *v1.EventsRequest, server v1.IndexedMap_EventsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

func (s *indexedMapServer) Entries(request *v1.EntriesRequest, server v1.IndexedMap_EntriesServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Entries(request, server)
}

var _ v1.IndexedMapServer = (*indexedMapServer)(nil)
