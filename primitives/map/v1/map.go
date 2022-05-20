// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Primitive = primitive.New[Map](clientFactory, func(server *grpc.Server, service *primitive.Service[Map], registry *primitive.Registry[Map]) {
	v1.RegisterMapManagerServer(server, newMapV1ManagerServer(service))
	v1.RegisterMapServer(server, newMapV1Server(registry))
})

// clientFactory is the map/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[Map], bool) {
	if mapClient, ok := client.(MapClient); ok {
		return primitive.NewClient[Map](mapClient.GetMap), true
	}
	return nil, false
}

type MapClient interface {
	GetMap(ctx context.Context, primitiveID runtimev1.PrimitiveId) (Map, error)
}

type Map interface {
	primitive.Primitive
	v1.MapServer
}
