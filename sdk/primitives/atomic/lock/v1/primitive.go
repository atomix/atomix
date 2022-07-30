// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/lock/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "AtomicLock"
	APIVersion = "v1"
)

var Type = runtime.NewType[lockv1.AtomicLockServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[lockv1.AtomicLockServer]) {
	lockv1.RegisterAtomicLockServer(server, newAtomicLockServer(delegate))
}

func resolve(client runtime.Client) (lockv1.AtomicLockServer, bool) {
	if provider, ok := client.(AtomicLockProvider); ok {
		return provider.AtomicLock(), true
	}
	return nil, false
}

type AtomicLockProvider interface {
	AtomicLock() lockv1.AtomicLockServer
}
