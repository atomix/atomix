// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "Lock"
	APIVersion = "v1"
)

var Type = primitive.NewType[lockv1.LockClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[lockv1.LockClient]) {
	lockv1.RegisterLockServer(server, newLockServer(manager))
}

func resolve(client runtime.Client) (primitive.Factory[lockv1.LockClient], bool) {
	if lock, ok := client.(LockProvider); ok {
		return lock.GetLock, true
	}
	return nil, false
}

type LockProvider interface {
	GetLock(string) lockv1.LockClient
}
