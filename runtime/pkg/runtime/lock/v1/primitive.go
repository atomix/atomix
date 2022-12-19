// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/atomix/api/pkg/runtime/lock/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
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
	lockv1.RegisterLockServer(server, newLockServer(runtime.NewPrimitiveClient[Lock](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Lock], bool) {
	if provider, ok := conn.(LockProvider); ok {
		return provider.NewLock, true
	}
	return nil, false
}

type Lock lockv1.LockServer

type LockProvider interface {
	NewLock(config []byte) (Lock, error)
}
