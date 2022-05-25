// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newLockServer(sessions *primitive.SessionManager[Lock]) v1.LockServer {
	return &lockServer{
		sessions: sessions,
	}
}

type lockServer struct {
	sessions *primitive.SessionManager[Lock]
}

func (s *lockServer) Lock(ctx context.Context, request *v1.LockRequest) (*v1.LockResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Lock(ctx, request)
}

func (s *lockServer) Unlock(ctx context.Context, request *v1.UnlockRequest) (*v1.UnlockResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Unlock(ctx, request)
}

func (s *lockServer) GetLock(ctx context.Context, request *v1.GetLockRequest) (*v1.GetLockResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.GetLock(ctx, request)
}

var _ v1.LockServer = (*lockServer)(nil)
