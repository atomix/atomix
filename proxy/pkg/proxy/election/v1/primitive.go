// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.election.v1.LeaderElection"

var Type = proxy.NewType[electionv1.LeaderElectionServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[electionv1.LeaderElectionServer]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (electionv1.LeaderElectionServer, bool, error) {
	if provider, ok := conn.(LeaderElectionProvider); ok {
		election, err := provider.NewLeaderElection(spec)
		return election, true, err
	}
	return nil, false, nil
}

type LeaderElectionProvider interface {
	NewLeaderElection(spec proxy.PrimitiveSpec) (electionv1.LeaderElectionServer, error)
}
