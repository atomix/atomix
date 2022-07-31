// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.election.v1.LeaderElection"

var Type = runtime.NewType[electionv1.LeaderElectionServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[electionv1.LeaderElectionServer]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (electionv1.LeaderElectionServer, error) {
	return conn.LeaderElection(config)
}
