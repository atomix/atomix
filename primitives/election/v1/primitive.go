// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.election.v1.LeaderElection"

var Kind = primitive.NewKind[electionv1.LeaderElectionServer](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[electionv1.LeaderElectionServer]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[electionv1.LeaderElectionServer], bool) {
	if election, ok := client.(LeaderElectionProvider); ok {
		return election.GetLeaderElection, true
	}
	return nil, false
}

type LeaderElectionProvider interface {
	GetLeaderElection(primitive.ID) electionv1.LeaderElectionServer
}
