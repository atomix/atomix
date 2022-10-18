// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/runtime/lock/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.lock.v1.Lock"

var Type = proxy.NewType[lockv1.LockServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[lockv1.LockServer]) {
	lockv1.RegisterLockServer(server, newLockServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (lockv1.LockServer, bool, error) {
	if provider, ok := conn.(LockProvider); ok {
		lock, err := provider.NewLock(spec)
		return lock, true, err
	}
	return nil, false, nil
}

type LockProvider interface {
	NewLock(spec proxy.PrimitiveSpec) (lockv1.LockServer, error)
}
