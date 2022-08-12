// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/lock/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.lock.v1.Lock"

var Type = runtime.NewType[lockv1.LockServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[lockv1.LockServer]) {
	lockv1.RegisterLockServer(server, newAtomicLockServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (lockv1.LockServer, error) {
	return conn.Lock(config)
}
