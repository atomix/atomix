// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/logging"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"github.com/atomix/runtime/sdk/pkg/stringer"
)

var log = logging.GetLogger()

const truncLen = 250

func newLeaderElectionServer(delegate *runtime.Delegate[electionv1.LeaderElectionServer]) electionv1.LeaderElectionServer {
	return &leaderElectionServer{
		delegate: delegate,
	}
}

type leaderElectionServer struct {
	delegate *runtime.Delegate[electionv1.LeaderElectionServer]
}

func (s *leaderElectionServer) Create(ctx context.Context, request *electionv1.CreateRequest) (*electionv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Create(request.ID.Name, request.Tags)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Create(ctx, request)
	if err != nil {
		log.Warnw("Create",
			logging.Stringer("CreateRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Create",
		logging.Stringer("CreateResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Close(ctx context.Context, request *electionv1.CloseRequest) (*electionv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Close(ctx, request)
	if err != nil {
		log.Warnw("Close",
			logging.Stringer("CloseRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Close",
		logging.Stringer("CloseResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Enter(ctx context.Context, request *electionv1.EnterRequest) (*electionv1.EnterResponse, error) {
	log.Debugw("Enter",
		logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Enter(ctx, request)
	if err != nil {
		log.Warnw("Enter",
			logging.Stringer("EnterRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Enter",
		logging.Stringer("EnterResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Withdraw(ctx context.Context, request *electionv1.WithdrawRequest) (*electionv1.WithdrawResponse, error) {
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Withdraw(ctx, request)
	if err != nil {
		log.Warnw("Withdraw",
			logging.Stringer("WithdrawRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Withdraw",
		logging.Stringer("WithdrawResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Anoint(ctx context.Context, request *electionv1.AnointRequest) (*electionv1.AnointResponse, error) {
	log.Debugw("Anoint",
		logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Anoint(ctx, request)
	if err != nil {
		log.Warnw("Anoint",
			logging.Stringer("AnointRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Anoint",
		logging.Stringer("AnointResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Promote(ctx context.Context, request *electionv1.PromoteRequest) (*electionv1.PromoteResponse, error) {
	log.Debugw("Promote",
		logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Promote(ctx, request)
	if err != nil {
		log.Warnw("Promote",
			logging.Stringer("PromoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Promote",
		logging.Stringer("PromoteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Demote(ctx context.Context, request *electionv1.DemoteRequest) (*electionv1.DemoteResponse, error) {
	log.Debugw("Demote",
		logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Demote(ctx, request)
	if err != nil {
		log.Warnw("Demote",
			logging.Stringer("DemoteRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Demote",
		logging.Stringer("DemoteResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Evict(ctx context.Context, request *electionv1.EvictRequest) (*electionv1.EvictResponse, error) {
	log.Debugw("Evict",
		logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.Evict(ctx, request)
	if err != nil {
		log.Warnw("Evict",
			logging.Stringer("EvictRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("Evict",
		logging.Stringer("EvictResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) GetTerm(ctx context.Context, request *electionv1.GetTermRequest) (*electionv1.GetTermResponse, error) {
	log.Debugw("GetTerm",
		logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	response, err := client.GetTerm(ctx, request)
	if err != nil {
		log.Warnw("GetTerm",
			logging.Stringer("GetTermRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return nil, err
	}
	log.Debugw("GetTerm",
		logging.Stringer("GetTermResponse", stringer.Truncate(response, truncLen)))
	return response, nil
}

func (s *leaderElectionServer) Watch(request *electionv1.WatchRequest, server electionv1.LeaderElection_WatchServer) error {
	log.Debugw("Watch",
		logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
		logging.String("State", "started"))
	client, err := s.delegate.Get(request.ID.Name)
	if err != nil {
		err = errors.ToProto(err)
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	err = client.Watch(request, server)
	if err != nil {
		log.Warnw("Watch",
			logging.Stringer("WatchRequest", stringer.Truncate(request, truncLen)),
			logging.Error("Error", err))
		return err
	}
	return nil
}

var _ electionv1.LeaderElectionServer = (*leaderElectionServer)(nil)
