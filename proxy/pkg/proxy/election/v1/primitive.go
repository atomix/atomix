// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/atomix/api/pkg/election/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	electiondriverv1 "github.com/atomix/atomix/driver/pkg/driver/election/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.election.v1.LeaderElection"

var Type = proxy.NewType[electionv1.LeaderElectionServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[electionv1.LeaderElectionServer]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (electionv1.LeaderElectionServer, bool, error) {
	if provider, ok := conn.(electiondriverv1.LeaderElectionProvider); ok {
		election, err := provider.NewLeaderElection(spec)
		return election, true, err
	}
	return nil, false, nil
}
