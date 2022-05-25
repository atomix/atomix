// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.election.v1.LeaderElection"

var Primitive = primitive.NewType[LeaderElectionClient, LeaderElection, *electionv1.LeaderElectionConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[LeaderElection]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(sessions))
}

func create(client LeaderElectionClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *electionv1.LeaderElectionConfig) (LeaderElection, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *electionv1.LeaderElectionConfig) (LeaderElection, error) {
		return client.GetLeaderElection(ctx, sessionID)
	}
}

type LeaderElectionClient interface {
	GetLeaderElection(ctx context.Context, sessionID primitivev1.SessionId) (LeaderElection, error)
}

type LeaderElection interface {
	primitive.Primitive
	electionv1.LeaderElectionServer
}
