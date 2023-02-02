// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/logging"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

type LeaderElectionProvider interface {
	NewLeaderElectionV1(ctx context.Context, id runtimev1.PrimitiveID) (LeaderElectionProxy, error)
}

func resolve(ctx context.Context, conn driver.Conn, id runtimev1.PrimitiveID) (LeaderElectionProxy, bool, error) {
	if provider, ok := conn.(LeaderElectionProvider); ok {
		counter, err := provider.NewLeaderElectionV1(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return counter, true, nil
	}
	return nil, false, nil
}

func NewLeaderElectionsServer(rt *runtime.Runtime) electionv1.LeaderElectionsServer {
	return &leaderElectionsServer{
		manager: runtime.NewPrimitiveManager[LeaderElectionProxy, *electionv1.Config](electionv1.PrimitiveType, resolve, rt),
	}
}

type leaderElectionsServer struct {
	manager runtime.PrimitiveManager[*electionv1.Config]
}

func (s *leaderElectionsServer) Create(ctx context.Context, request *electionv1.CreateRequest) (*electionv1.CreateResponse, error) {
	log.Debugw("Create",
		logging.Trunc64("CreateRequest", request))
	config, err := s.manager.Create(ctx, request.ID, request.Tags)
	if err != nil {
		log.Warnw("Create",
			logging.Trunc64("CreateRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.CreateResponse{
		Config: *config,
	}
	log.Debugw("Create",
		logging.Trunc64("CreateResponse", response))
	return response, nil
}

func (s *leaderElectionsServer) Close(ctx context.Context, request *electionv1.CloseRequest) (*electionv1.CloseResponse, error) {
	log.Debugw("Close",
		logging.Trunc64("CloseRequest", request))
	err := s.manager.Close(ctx, request.ID)
	if err != nil {
		log.Warnw("Close",
			logging.Trunc64("CloseRequest", request),
			logging.Error("Error", err))
		return nil, err
	}
	response := &electionv1.CloseResponse{}
	log.Debugw("Close",
		logging.Trunc64("CloseResponse", response))
	return response, nil
}

var _ electionv1.LeaderElectionsServer = (*leaderElectionsServer)(nil)
