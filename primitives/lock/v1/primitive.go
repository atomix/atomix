// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.lock.v1.Lock"

var Kind = primitive.NewKind[lockv1.LockServer](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[lockv1.LockServer]) {
	lockv1.RegisterLockServer(server, newLockServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[lockv1.LockServer], bool) {
	if lock, ok := client.(LockProvider); ok {
		return lock.GetLock, true
	}
	return nil, false
}

type LockProvider interface {
	GetLock(primitive.ID) lockv1.LockServer
}
