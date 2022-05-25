// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.counter.v1.Counter"

var Primitive = primitive.NewKind[CounterClient, Counter, *counterv1.CounterConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[Counter]) {
	counterv1.RegisterCounterServer(server, newCounterServer(sessions))
}

func create(client CounterClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *counterv1.CounterConfig) (Counter, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *counterv1.CounterConfig) (Counter, error) {
		return client.GetCounter(ctx, sessionID)
	}
}

type CounterClient interface {
	GetCounter(ctx context.Context, sessionID primitivev1.SessionId) (Counter, error)
}

type Counter interface {
	primitive.Primitive
	counterv1.CounterServer
}
