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

func newLeaderElectionV1ManagerServer(proxies *primitive.Service[LeaderElection]) v1.LeaderElectionManagerServer {
	return &leaderElectionV1ManagerServer{
		proxies: proxies,
	}
}

type leaderElectionV1ManagerServer struct {
	proxies *primitive.Service[LeaderElection]
}

func (s *leaderElectionV1ManagerServer) Create(ctx context.Context, request *v1.CreateRequest) (*v1.CreateResponse, error) {
	namespace, err := s.proxies.GetCluster(ctx, request.Headers.Cluster)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	err = namespace.CreateProxy(ctx, request.Name)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return &v1.CreateResponse{}, nil
}

func (s *leaderElectionV1ManagerServer) Close(ctx context.Context, request *v1.CloseRequest) (*v1.CloseResponse, error) {
	namespace, err := s.proxies.GetCluster(ctx, request.Headers.Cluster)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	err = namespace.CloseProxy(ctx, request.Name)
	if err != nil {
		return nil, errors.ToProto(err)
	}
	return &v1.CloseResponse{}, nil
}

var _ v1.LeaderElectionManagerServer = (*leaderElectionV1ManagerServer)(nil)
