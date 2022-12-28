// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	electionv1 "github.com/atomix/atomix/api/runtime/election/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
)

const (
	Name       = "LeaderElection"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

type LeaderElection electionv1.LeaderElectionServer

type LeaderElectionProvider interface {
	NewLeaderElectionV1(primitiveID runtimev1.PrimitiveID) (LeaderElection, error)
}

type ConfigurableLeaderElectionProvider[S any] interface {
	NewLeaderElectionV1(primitiveID runtimev1.PrimitiveID, spec S) (LeaderElection, error)
}
