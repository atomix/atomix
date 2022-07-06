// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "LeaderElection"
	APIVersion = "v1"
)

var Type = primitive.NewType[electionv1.LeaderElectionClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[electionv1.LeaderElectionClient]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(manager))
}

func resolve(client runtime.Client) (primitive.Factory[electionv1.LeaderElectionClient], bool) {
	if election, ok := client.(LeaderElectionProvider); ok {
		return election.GetLeaderElection, true
	}
	return nil, false
}

type LeaderElectionProvider interface {
	GetLeaderElection(string) electionv1.LeaderElectionClient
}
