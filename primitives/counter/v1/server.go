// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/primitive/counter/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newCounterServer(sessions *primitive.SessionManager[Counter]) v1.CounterServer {
	return &counterServer{
		sessions: sessions,
	}
}

type counterServer struct {
	sessions *primitive.SessionManager[Counter]
}

func (s *counterServer) Set(ctx context.Context, request *v1.SetRequest) (*v1.SetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Set(ctx, request)
}

func (s *counterServer) Get(ctx context.Context, request *v1.GetRequest) (*v1.GetResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Get(ctx, request)
}

func (s *counterServer) Increment(ctx context.Context, request *v1.IncrementRequest) (*v1.IncrementResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Increment(ctx, request)
}

func (s *counterServer) Decrement(ctx context.Context, request *v1.DecrementRequest) (*v1.DecrementResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Decrement(ctx, request)
}

var _ v1.CounterServer = (*counterServer)(nil)
