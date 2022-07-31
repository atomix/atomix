// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/lock/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.lock.v1.AtomicLock"

var Type = runtime.NewType[lockv1.AtomicLockServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[lockv1.AtomicLockServer]) {
	lockv1.RegisterAtomicLockServer(server, newAtomicLockServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (lockv1.AtomicLockServer, error) {
	return conn.AtomicLock(config)
}
