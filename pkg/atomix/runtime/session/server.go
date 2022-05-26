// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
)

func NewServer(sessions Manager) runtimev1.SessionServiceServer {
	return &sessionServiceServer{
		sessions: sessions,
	}
}

type sessionServiceServer struct {
	sessions Manager
}

func (s *sessionServiceServer) GetSession(ctx context.Context, request *runtimev1.GetSessionRequest) (*runtimev1.GetSessionResponse, error) {
	log.Debugw("GetSession",
		logging.Stringer("GetSessionRequest", request))

	session, ok := s.sessions.Store().Get(&request.SessionID)
	if !ok {
		err := errors.NewNotFound("session '%s' not found", request.SessionID)
		log.Warnw("GetSession",
			logging.Stringer("GetSessionRequest", request),
			logging.Error("Error", err))
		return nil, errors.ToProto(err)
	}

	response := &runtimev1.GetSessionResponse{
		Session: session,
	}
	log.Debugw("GetSession",
		logging.Stringer("GetSessionResponse", response))
	return response, nil
}

func (s *sessionServiceServer) ListSessions(ctx context.Context, request *runtimev1.ListSessionsRequest) (*runtimev1.ListSessionsResponse, error) {
	log.Debugw("ListSessions",
		logging.Stringer("ListSessionsRequest", request))

	sessions := s.sessions.Store().List()
	response := &runtimev1.ListSessionsResponse{
		Sessions: sessions,
	}
	log.Debugw("ListSessions",
		logging.Stringer("ListSessionsResponse", response))
	return response, nil
}

var _ runtimev1.SessionServiceServer = (*sessionServiceServer)(nil)
