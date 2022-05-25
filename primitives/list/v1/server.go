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

func newListServer(sessions *primitive.SessionManager[List]) v1.ListServer {
	return &listServer{
		sessions: sessions,
	}
}

type listServer struct {
	sessions *primitive.SessionManager[List]
}

func (s *listServer) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Size(ctx, request)
}

func (s *listServer) Append(ctx context.Context, request *v1.AppendRequest) (*v1.AppendResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Append(ctx, request)
}

func (s *listServer) Insert(ctx context.Context, request *v1.InsertRequest) (*v1.InsertResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Insert(ctx, request)
}

func (s *listServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Get(ctx, request)
}

func (s *listServer) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Set(ctx, request)
}

func (s *listServer) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Remove(ctx, request)
}

func (s *listServer) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Clear(ctx, request)
}

func (s *listServer) Events(request *v1.EventsRequest, server v1.List_EventsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Events(request, server)
}

func (s *listServer) Elements(request *v1.ElementsRequest, server v1.List_ElementsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Elements(request, server)
}

var _ v1.ListServer = (*listServer)(nil)
