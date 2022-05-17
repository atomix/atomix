// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Atom = primitive.New[Counter](clientFactory, func(server *grpc.Server, service *primitive.Service[Counter], registry *primitive.Registry[Counter]) {
	v1.RegisterCounterManagerServer(server, newCounterV1ManagerServer(service))
	v1.RegisterCounterServer(server, newCounterV1Server(registry))
})

// clientFactory is the counter/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[Counter], bool) {
	if counterClient, ok := client.(CounterClient); ok {
		return primitive.NewClient[Counter](counterClient.GetCounter), true
	}
	return nil, false
}

type CounterClient interface {
	GetCounter(ctx context.Context, name string) (Counter, error)
}

type Counter interface {
	primitive.Primitive
	v1.CounterServer
}
