// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/primitive/value/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newValueServer(sessions *primitive.SessionManager[Value]) v1.ValueServer {
	return &valueServer{
		sessions: sessions,
	}
}

type valueServer struct {
	sessions *primitive.SessionManager[Value]
}

func (s *valueServer) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Set(ctx, request)
}

func (s *valueServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Get(ctx, request)
}

func (s *valueServer) Events(request *v1.EventsRequest, server v1.Value_EventsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Events(request, server)
}

var _ v1.ValueServer = (*valueServer)(nil)
