// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

var Atom = atom.New[Counter](clientFactory, func(server *grpc.Server, service *atom.Service[Counter], registry *atom.Registry[Counter]) {
	v1.RegisterCounterManagerServer(server, newCounterV1ManagerServer(service))
	v1.RegisterCounterServer(server, newCounterV1Server(registry))
})

// clientFactory is the counter/v1 client factory
var clientFactory = func(client driver.Client) (*atom.Client[Counter], bool) {
	if counterClient, ok := client.(CounterClient); ok {
		return atom.NewClient[Counter](counterClient.GetCounter), true
	}
	return nil, false
}

type CounterClient interface {
	GetCounter(ctx context.Context, name string) (Counter, error)
}

type Counter interface {
	atom.Atom
	v1.CounterServer
}
