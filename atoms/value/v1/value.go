// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

var Atom = atom.New[Value](clientFactory, func(server *grpc.Server, service *atom.Service[Value], registry *atom.Registry[Value]) {
	v1.RegisterValueManagerServer(server, newValueV1ManagerServer(service))
	v1.RegisterValueServer(server, newValueV1Server(registry))
})

// clientFactory is the value/v1 client factory
var clientFactory = func(client driver.Client) (*atom.Client[Value], bool) {
	if valueClient, ok := client.(ValueClient); ok {
		return atom.NewClient[Value](valueClient.GetValue), true
	}
	return nil, false
}

type ValueClient interface {
	GetValue(ctx context.Context, name string) (Value, error)
}

type Value interface {
	atom.Atom
	v1.ValueServer
}
