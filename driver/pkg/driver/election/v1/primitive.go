// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	electionv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/election/v1"
)

type LeaderElectionProxy electionv1.LeaderElectionServer

type LeaderElectionProvider interface {
	NewLeaderElection(spec driver.PrimitiveSpec) (LeaderElectionProxy, error)
}
