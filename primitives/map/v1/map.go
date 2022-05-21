// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.map.v1.Map"

var Primitive = primitive.NewType[Map](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[Map], bool) {
	if mapClient, ok := client.(MapClient); ok {
		return primitive.NewClient[Map](mapClient.GetMap), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[Map], registry *primitive.Registry[Map]) {
	mapv1.RegisterMapServer(server, newMapV1Server(registry))
}

type MapClient interface {
	GetMap(ctx context.Context, primitiveID runtimev1.ObjectId) (Map, error)
}

type Map interface {
	primitive.Primitive
	mapv1.MapServer
}
