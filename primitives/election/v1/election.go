// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Atom = primitive.New[LeaderElection](clientFactory, func(server *grpc.Server, service *primitive.Service[LeaderElection], registry *primitive.Registry[LeaderElection]) {
	v1.RegisterLeaderElectionManagerServer(server, newLeaderElectionV1ManagerServer(service))
	v1.RegisterLeaderElectionServer(server, newLeaderElectionV1Server(registry))
})

// clientFactory is the election/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[LeaderElection], bool) {
	if electionClient, ok := client.(LeaderElectionClient); ok {
		return primitive.NewClient[LeaderElection](electionClient.GetLeaderElection), true
	}
	return nil, false
}

type LeaderElectionClient interface {
	GetLeaderElection(ctx context.Context, name string) (LeaderElection, error)
}

type LeaderElection interface {
	primitive.Primitive
	v1.LeaderElectionServer
}
