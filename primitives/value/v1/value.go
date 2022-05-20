// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Primitive = primitive.New[Value](clientFactory, func(server *grpc.Server, service *primitive.Service[Value], registry *primitive.Registry[Value]) {
	v1.RegisterValueManagerServer(server, newValueV1ManagerServer(service))
	v1.RegisterValueServer(server, newValueV1Server(registry))
})

// clientFactory is the value/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[Value], bool) {
	if valueClient, ok := client.(ValueClient); ok {
		return primitive.NewClient[Value](valueClient.GetValue), true
	}
	return nil, false
}

type ValueClient interface {
	GetValue(ctx context.Context, primitiveID runtimev1.PrimitiveId) (Value, error)
}

type Value interface {
	primitive.Primitive
	v1.ValueServer
}
