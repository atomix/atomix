// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newLeaderElectionServer(proxies *primitive.ProxyManager[LeaderElection]) v1.LeaderElectionServer {
	return &leaderElectionServer{
		proxies: proxies,
	}
}

type leaderElectionServer struct {
	proxies *primitive.ProxyManager[LeaderElection]
}

func (s *leaderElectionServer) Enter(ctx context.Context, request *v1.EnterRequest) (*v1.EnterResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Enter(ctx, request)
}

func (s *leaderElectionServer) Withdraw(ctx context.Context, request *v1.WithdrawRequest) (*v1.WithdrawResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Withdraw(ctx, request)
}

func (s *leaderElectionServer) Anoint(ctx context.Context, request *v1.AnointRequest) (*v1.AnointResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Anoint(ctx, request)
}

func (s *leaderElectionServer) Promote(ctx context.Context, request *v1.PromoteRequest) (*v1.PromoteResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Promote(ctx, request)
}

func (s *leaderElectionServer) Evict(ctx context.Context, request *v1.EvictRequest) (*v1.EvictResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.Evict(ctx, request)
}

func (s *leaderElectionServer) GetTerm(ctx context.Context, request *v1.GetTermRequest) (*v1.GetTermResponse, error) {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return proxy.GetTerm(ctx, request)
}

func (s *leaderElectionServer) Events(request *v1.EventsRequest, server v1.LeaderElection_EventsServer) error {
	proxy, err := s.proxies.GetProxy(request.Headers.PrimitiveId)
	if err != nil {
		return errors.ToProto(err)
	}
	return proxy.Events(request, server)
}

var _ v1.LeaderElectionServer = (*leaderElectionServer)(nil)
