// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.lock.v1.Lock"

var Primitive = primitive.NewType[Lock](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[Lock], bool) {
	if lockClient, ok := client.(LockClient); ok {
		return primitive.NewClient[Lock](lockClient.GetLock), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[Lock], registry *primitive.Registry[Lock]) {
	lockv1.RegisterLockServer(server, newLockV1Server(registry))
}

type LockClient interface {
	GetLock(ctx context.Context, primitiveID runtimev1.ObjectId) (Lock, error)
}

type Lock interface {
	primitive.Primitive
	lockv1.LockServer
}
