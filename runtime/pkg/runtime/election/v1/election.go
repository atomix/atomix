// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

var log = logging.GetLogger()

type LeaderElectionProxy interface {
	runtime.PrimitiveProxy
	// Enter enters the leader election
	Enter(context.Context, *electionv1.EnterRequest) (*electionv1.EnterResponse, error)
	// Withdraw withdraws a candidate from the leader election
	Withdraw(context.Context, *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error)
	// Anoint anoints a candidate leader
	Anoint(context.Context, *electionv1.AnointRequest) (*electionv1.AnointResponse, error)
	// Promote promotes a candidate
	Promote(context.Context, *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error)
	// Demote demotes a candidate
	Demote(context.Context, *electionv1.DemoteRequest) (*electionv1.DemoteResponse, error)
	// Evict evicts a candidate from the election
	Evict(context.Context, *electionv1.EvictRequest) (*electionv1.EvictResponse, error)
	// GetTerm gets the current leadership term
	GetTerm(context.Context, *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error)
	// Watch watches the election for events
	Watch(*electionv1.WatchRequest, electionv1.LeaderElection_WatchServer) error
}

func NewLeaderElectionServer(rt *runtime.Runtime) electionv1.LeaderElectionServer {
	return &leaderElectionServer{
		LeaderElectionsServer: NewLeaderElectionsServer(rt),
		primitives:            runtime.NewPrimitiveRegistry[LeaderElectionProxy](electionv1.PrimitiveType, rt),
	}
}

type leaderElectionServer struct {
	electionv1.LeaderElectionsServer
	primitives runtime.PrimitiveRegistry[LeaderElectionProxy]
}

func (s *leaderElectionServer) Enter(ctx context.Context, request *electionv1.EnterRequest) (*electionv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Trunc64("EnterRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Enter",
			logging.Trunc64("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Enter(ctx, request)
	if err != nil {
		log.Debugw("Enter",
			logging.Trunc64("EnterRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Enter",
		logging.Trunc64("EnterResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Withdraw(ctx context.Context, request *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Trunc64("WithdrawRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Trunc64("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Withdraw(ctx, request)
	if err != nil {
		log.Debugw("Withdraw",
			logging.Trunc64("WithdrawRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Withdraw",
		logging.Trunc64("WithdrawResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Anoint(ctx context.Context, request *electionv1.AnointRequest) (*electionv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Trunc64("AnointRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Anoint",
			logging.Trunc64("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Anoint(ctx, request)
	if err != nil {
		log.Debugw("Anoint",
			logging.Trunc64("AnointRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Anoint",
		logging.Trunc64("AnointResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Promote(ctx context.Context, request *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Trunc64("PromoteRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Promote",
			logging.Trunc64("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Promote(ctx, request)
	if err != nil {
		log.Debugw("Promote",
			logging.Trunc64("PromoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Promote",
		logging.Trunc64("PromoteResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Demote(ctx context.Context, request *electionv1.DemoteRequest) (*electionv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Trunc64("DemoteRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Demote",
			logging.Trunc64("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Demote(ctx, request)
	if err != nil {
		log.Debugw("Demote",
			logging.Trunc64("DemoteRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Demote",
		logging.Trunc64("DemoteResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Evict(ctx context.Context, request *electionv1.EvictRequest) (*electionv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Trunc64("EvictRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Evict",
			logging.Trunc64("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Evict(ctx, request)
	if err != nil {
		log.Debugw("Evict",
			logging.Trunc64("EvictRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Evict",
		logging.Trunc64("EvictResponse", response))
	return response, nil
}

func (s *leaderElectionServer) GetTerm(ctx context.Context, request *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Trunc64("GetTermRequest", request))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Trunc64("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.GetTerm(ctx, request)
	if err != nil {
		log.Debugw("GetTerm",
			logging.Trunc64("GetTermRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("GetTerm",
		logging.Trunc64("GetTermResponse", response))
	return response, nil
}

func (s *leaderElectionServer) Watch(request *electionv1.WatchRequest, server electionv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Trunc64("WatchRequest", request),
		logging.String("State", "started"))
	client, err := s.primitives.Get(request.ID)
	if err != nil {
		log.Warnw("Watch",
			logging.Trunc64("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	err = client.Watch(request, server)
	if err != nil {
		log.Debugw("Watch",
			logging.Trunc64("WatchRequest", request),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ electionv1.LeaderElectionServer = (*leaderElectionServer)(nil)
