// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
)

func newLeaderElectionServer(proxies *primitive.Manager[electionv1.LeaderElectionServer]) electionv1.LeaderElectionServer {
	return &leaderElectionServer{
		proxies: proxies,
	}
}

type leaderElectionServer struct {
	proxies *primitive.Manager[electionv1.LeaderElectionServer]
}

func (s *leaderElectionServer) Create(ctx context.Context, request *electionv1.CreateRequest) (*electionv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", request))
	proxy, err := s.proxies.Connect(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Create(ctx, request)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Close(ctx context.Context, request *electionv1.CloseRequest) (*electionv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", request))
	proxy, err := s.proxies.Close(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Close(ctx, request)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Enter(ctx context.Context, request *electionv1.EnterRequest) (*electionv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Enter(ctx, request)
	if err != nil {
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Enter",
		logging.Stringer("EnterResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Withdraw(ctx context.Context, request *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Withdraw(ctx, request)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Anoint(ctx context.Context, request *electionv1.AnointRequest) (*electionv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Anoint(ctx, request)
	if err != nil {
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Anoint",
		logging.Stringer("AnointResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Promote(ctx context.Context, request *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Promote(ctx, request)
	if err != nil {
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Promote",
		logging.Stringer("PromoteResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Evict(ctx context.Context, request *electionv1.EvictRequest) (*electionv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.Evict(ctx, request)
	if err != nil {
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Evict",
		logging.Stringer("EvictResponse", response))
	return response, nil
}

func (s *leaderElectionServer) GetTerm(ctx context.Context, request *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", request))
	proxy, err := s.proxies.Get(ctx)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := proxy.GetTerm(ctx, request)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("GetTerm",
		logging.Stringer("GetTermResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Events(request *electionv1.EventsRequest, server electionv1.LeaderElection_EventsServer) error {
	log.Debugw("Events",
		logging.Stringer("EventsRequest", request))
	proxy, err := s.proxies.Get(server.Context())
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = proxy.Events(request, server)
	if err != nil {
		log.Warnw("Events",
			logging.Stringer("EventsRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ electionv1.LeaderElectionServer = (*leaderElectionServer)(nil)
