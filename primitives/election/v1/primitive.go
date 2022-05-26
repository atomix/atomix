// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.election.v1.LeaderElection"

var Kind = primitive.NewKind[LeaderElectionClient, LeaderElection, *electionv1.LeaderElectionConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[LeaderElection]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(proxies))
}

func create(client LeaderElectionClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *electionv1.LeaderElectionConfig) (LeaderElection, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *electionv1.LeaderElectionConfig) (LeaderElection, error) {
		return client.GetLeaderElection(ctx, primitiveID)
	}
}

type LeaderElectionClient interface {
	GetLeaderElection(ctx context.Context, primitiveID atomixv1.PrimitiveId) (LeaderElection, error)
}

type LeaderElection interface {
	primitive.Proxy
	electionv1.LeaderElectionServer
}
