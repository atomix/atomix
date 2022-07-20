// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/runtime/lock/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Lock"
	APIVersion = "v1"
)

var Type = runtime.NewType[lockv1.LockServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[lockv1.LockServer]) {
	lockv1.RegisterLockServer(server, newLockServer(delegate))
}

func resolve(client runtime.Client) (lockv1.LockServer, bool) {
	if provider, ok := client.(LockProvider); ok {
		return provider.Lock(), true
	}
	return nil, false
}

type LockProvider interface {
	Lock() lockv1.LockServer
}
