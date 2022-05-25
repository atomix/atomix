// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newSetServer(sessions *primitive.SessionManager[Set]) v1.SetServer {
	return &setServer{
		sessions: sessions,
	}
}

type setServer struct {
	sessions *primitive.SessionManager[Set]
}

func (s *setServer) Size(ctx context.Context, request *v1.SizeRequest) (*v1.SizeResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Size(ctx, request)
}

func (s *setServer) Contains(ctx context.Context, request *v1.ContainsRequest) (*v1.ContainsResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Contains(ctx, request)
}

func (s *setServer) Add(ctx context.Context, request *v1.AddRequest) (*v1.AddResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Add(ctx, request)
}

func (s *setServer) Remove(ctx context.Context, request *v1.RemoveRequest) (*v1.RemoveResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Remove(ctx, request)
}

func (s *setServer) Clear(ctx context.Context, request *v1.ClearRequest) (*v1.ClearResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Clear(ctx, request)
}

func (s *setServer) Events(request *v1.EventsRequest, server v1.Set_EventsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Events(request, server)
}

func (s *setServer) Elements(request *v1.ElementsRequest, server v1.Set_ElementsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Elements(request, server)
}

var _ v1.SetServer = (*setServer)(nil)
