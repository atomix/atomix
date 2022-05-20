// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/lock/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Primitive = primitive.New[Lock](clientFactory, func(server *grpc.Server, service *primitive.Service[Lock], registry *primitive.Registry[Lock]) {
	v1.RegisterLockManagerServer(server, newLockV1ManagerServer(service))
	v1.RegisterLockServer(server, newLockV1Server(registry))
})

// clientFactory is the lock/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[Lock], bool) {
	if lockClient, ok := client.(LockClient); ok {
		return primitive.NewClient[Lock](lockClient.GetLock), true
	}
	return nil, false
}

type LockClient interface {
	GetLock(ctx context.Context, primitiveID runtimev1.PrimitiveId) (Lock, error)
}

type Lock interface {
	primitive.Primitive
	v1.LockServer
}
