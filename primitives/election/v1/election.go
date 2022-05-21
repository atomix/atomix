// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.election.v1.LeaderElection"

var Primitive = primitive.NewType[LeaderElection](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[LeaderElection], bool) {
	if electionClient, ok := client.(LeaderElectionClient); ok {
		return primitive.NewClient[LeaderElection](electionClient.GetLeaderElection), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[LeaderElection], registry *primitive.Registry[LeaderElection]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionV1Server(registry))
}

type LeaderElectionClient interface {
	GetLeaderElection(ctx context.Context, primitiveID runtimev1.ObjectId) (LeaderElection, error)
}

type LeaderElection interface {
	primitive.Primitive
	electionv1.LeaderElectionServer
}
