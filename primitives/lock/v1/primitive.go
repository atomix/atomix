// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.lock.v1.Lock"

var Kind = primitive.NewKind[LockClient, Lock, *lockv1.LockConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[Lock]) {
	lockv1.RegisterLockServer(server, newLockServer(proxies))
}

func create(client LockClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *lockv1.LockConfig) (Lock, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *lockv1.LockConfig) (Lock, error) {
		return client.GetLock(ctx, primitiveID)
	}
}

type LockClient interface {
	GetLock(ctx context.Context, primitiveID atomixv1.PrimitiveId) (Lock, error)
}

type Lock interface {
	primitive.Proxy
	lockv1.LockServer
}
