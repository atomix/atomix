// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.counter.v1.Counter"

var Primitive = primitive.NewType[Counter](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[Counter], bool) {
	if counterClient, ok := client.(CounterClient); ok {
		return primitive.NewClient[Counter](counterClient.GetCounter), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[Counter], registry *primitive.Registry[Counter]) {
	counterv1.RegisterCounterServer(server, newCounterV1Server(registry))
}

type CounterClient interface {
	GetCounter(ctx context.Context, primitiveID runtimev1.ObjectId) (Counter, error)
}

type Counter interface {
	primitive.Primitive
	counterv1.CounterServer
}
