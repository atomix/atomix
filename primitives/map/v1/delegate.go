// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/primitive/map/v1"
)

func newDelegatingMap(delegate Map) Map {
	return &delegatingMap{
		delegate: delegate,
	}
}

type delegatingMap struct {
	delegate Map
}

func (s *delegatingMap) Size(ctx context.Context, request *mapv1.SizeRequest) (*mapv1.SizeResponse, error) {
	return s.delegate.Size(ctx, request)
}

func (s *delegatingMap) Put(ctx context.Context, request *mapv1.PutRequest) (*mapv1.PutResponse, error) {
	return s.delegate.Put(ctx, request)
}

func (s *delegatingMap) Get(ctx context.Context, request *mapv1.GetRequest) (*mapv1.GetResponse, error) {
	return s.delegate.Get(ctx, request)
}

func (s *delegatingMap) Remove(ctx context.Context, request *mapv1.RemoveRequest) (*mapv1.RemoveResponse, error) {
	return s.delegate.Remove(ctx, request)
}

func (s *delegatingMap) Clear(ctx context.Context, request *mapv1.ClearRequest) (*mapv1.ClearResponse, error) {
	return s.delegate.Clear(ctx, request)
}

func (s *delegatingMap) Events(request *mapv1.EventsRequest, server mapv1.Map_EventsServer) error {
	return s.delegate.Events(request, server)
}

func (s *delegatingMap) Entries(request *mapv1.EntriesRequest, server mapv1.Map_EntriesServer) error {
	return s.delegate.Entries(request, server)
}

func (s *delegatingMap) Close(ctx context.Context) error {
	return s.delegate.Close(ctx)
}

var _ Map = (*delegatingMap)(nil)
