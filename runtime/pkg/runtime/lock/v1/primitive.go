// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/atomix/api/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
	"google.golang.org/grpc"
)

const (
	Name       = "Lock"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	lockv1.RegisterLockServer(server, newLockServer(runtime.NewPrimitiveManager[Lock](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Lock], bool) {
	if provider, ok := conn.(LockProvider); ok {
		return provider.NewLock, true
	}
	return nil, false
}

type Lock lockv1.LockServer

type LockProvider interface {
	NewLock(spec runtimev1.Primitive) (Lock, error)
}
