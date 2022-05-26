// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.counter.v1.Counter"

var Kind = primitive.NewKind[CounterClient, Counter, *counterv1.CounterConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[Counter]) {
	counterv1.RegisterCounterServer(server, newCounterServer(proxies))
}

func create(client CounterClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *counterv1.CounterConfig) (Counter, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *counterv1.CounterConfig) (Counter, error) {
		return client.GetCounter(ctx, primitiveID)
	}
}

type CounterClient interface {
	GetCounter(ctx context.Context, primitiveID atomixv1.PrimitiveId) (Counter, error)
}

type Counter interface {
	primitive.Proxy
	counterv1.CounterServer
}
