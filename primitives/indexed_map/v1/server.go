// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/primitive/indexed_map/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newIndexedMapServer(sessions *primitive.SessionManager[IndexedMap]) v1.IndexedMapServer {
	return &indexedMapServer{
		sessions: sessions,
	}
}

type indexedMapServer struct {
	sessions *primitive.SessionManager[IndexedMap]
}

func (s *indexedMapServer) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Size(ctx, request)
}

func (s *indexedMapServer) Put(ctx context.Context, request *v1.PutRequest) (*v1.PutResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Put(ctx, request)
}

func (s *indexedMapServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Get(ctx, request)
}

func (s *indexedMapServer) FirstEntry(ctx context.Context, request *v1.FirstEntryRequest) (*v1.FirstEntryResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.FirstEntry(ctx, request)
}

func (s *indexedMapServer) LastEntry(ctx context.Context, request *v1.LastEntryRequest) (*v1.LastEntryResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.LastEntry(ctx, request)
}

func (s *indexedMapServer) PrevEntry(ctx context.Context, request *v1.PrevEntryRequest) (*v1.PrevEntryResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.PrevEntry(ctx, request)
}

func (s *indexedMapServer) NextEntry(ctx context.Context, request *v1.NextEntryRequest) (*v1.NextEntryResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.NextEntry(ctx, request)
}

func (s *indexedMapServer) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Remove(ctx, request)
}

func (s *indexedMapServer) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Clear(ctx, request)
}

func (s *indexedMapServer) Events(request *v1.EventsRequest, server v1.IndexedMap_EventsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Events(request, server)
}

func (s *indexedMapServer) Entries(request *v1.EntriesRequest, server v1.IndexedMap_EntriesServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Entries(request, server)
}

var _ v1.IndexedMapServer = (*indexedMapServer)(nil)
