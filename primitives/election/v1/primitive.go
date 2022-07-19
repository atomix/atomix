// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/runtime/api/atomix/election/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "LeaderElection"
	APIVersion = "v1"
)

var Type = runtime.NewType[electionv1.LeaderElectionServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[electionv1.LeaderElectionServer]) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(delegate))
}

func resolve(client runtime.Client) (electionv1.LeaderElectionServer, bool) {
	if provider, ok := client.(LeaderElectionProvider); ok {
		return provider.LeaderElection(), true
	}
	return nil, false
}

type LeaderElectionProvider interface {
	LeaderElection() electionv1.LeaderElectionServer
}
