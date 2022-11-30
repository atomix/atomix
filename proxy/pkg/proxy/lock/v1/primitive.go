// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	lockdriverv1 "github.com/atomix/atomix/driver/pkg/driver/lock/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	lockv1 "github.com/atomix/atomix/runtime/api/atomix/runtime/lock/v1"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.lock.v1.Lock"

var Type = proxy.NewType[lockv1.LockServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[lockv1.LockServer]) {
	lockv1.RegisterLockServer(server, newLockServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (lockv1.LockServer, bool, error) {
	if provider, ok := conn.(lockdriverv1.LockProvider); ok {
		lock, err := provider.NewLock(spec)
		return lock, true, err
	}
	return nil, false, nil
}
