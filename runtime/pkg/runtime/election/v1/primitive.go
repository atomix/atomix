// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/atomix/api/pkg/runtime/election/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "LeaderElection"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt *runtime.Runtime) {
	electionv1.RegisterLeaderElectionServer(server, newLeaderElectionServer(runtime.NewPrimitiveClient[LeaderElection](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[LeaderElection], bool) {
	if provider, ok := conn.(LeaderElectionProvider); ok {
		return provider.NewLeaderElection, true
	}
	return nil, false
}

type LeaderElection electionv1.LeaderElectionServer

type LeaderElectionProvider interface {
	NewLeaderElection(config []byte) (LeaderElection, error)
}
