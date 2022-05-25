// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/primitive"
)

func newLeaderElectionServer(sessions *primitive.SessionManager[LeaderElection]) v1.LeaderElectionServer {
	return &leaderElectionServer{
		sessions: sessions,
	}
}

type leaderElectionServer struct {
	sessions *primitive.SessionManager[LeaderElection]
}

func (s *leaderElectionServer) Enter(ctx context.Context, request *v1.EnterRequest) (*v1.EnterResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Enter(ctx, request)
}

func (s *leaderElectionServer) Withdraw(ctx context.Context, request *v1.WithdrawRequest) (*v1.WithdrawResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Withdraw(ctx, request)
}

func (s *leaderElectionServer) Anoint(ctx context.Context, request *v1.AnointRequest) (*v1.AnointResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Anoint(ctx, request)
}

func (s *leaderElectionServer) Promote(ctx context.Context, request *v1.PromoteRequest) (*v1.PromoteResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Promote(ctx, request)
}

func (s *leaderElectionServer) Evict(ctx context.Context, request *v1.EvictRequest) (*v1.EvictResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.Evict(ctx, request)
}

func (s *leaderElectionServer) GetTerm(ctx context.Context, request *v1.GetTermRequest) (*v1.GetTermResponse, error) {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return session.GetTerm(ctx, request)
}

func (s *leaderElectionServer) Events(request *v1.EventsRequest, server v1.LeaderElection_EventsServer) error {
	session, err := s.sessions.GetSession(request.Headers.Session)
	if err != nil {
		return errors.ToProto(err)
	}
	return session.Events(request, server)
}

var _ v1.LeaderElectionServer = (*leaderElectionServer)(nil)
