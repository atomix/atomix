// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	lockv1 "github.com/atomix/runtime/api/atomix/lock/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.lock.v1.Lock"

var Primitive = primitive.NewKind[LockClient, Lock, *lockv1.LockConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[Lock]) {
	lockv1.RegisterLockServer(server, newLockServer(sessions))
}

func create(client LockClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *lockv1.LockConfig) (Lock, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *lockv1.LockConfig) (Lock, error) {
		return client.GetLock(ctx, sessionID)
	}
}

type LockClient interface {
	GetLock(ctx context.Context, sessionID primitivev1.SessionId) (Lock, error)
}

type Lock interface {
	primitive.Primitive
	lockv1.LockServer
}
