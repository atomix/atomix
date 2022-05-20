// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Primitive = primitive.New[Set](clientFactory, func(server *grpc.Server, service *primitive.Service[Set], registry *primitive.Registry[Set]) {
	v1.RegisterSetManagerServer(server, newSetV1ManagerServer(service))
	v1.RegisterSetServer(server, newSetV1Server(registry))
})

// clientFactory is the set/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[Set], bool) {
	if setClient, ok := client.(SetClient); ok {
		return primitive.NewClient[Set](setClient.GetSet), true
	}
	return nil, false
}

type SetClient interface {
	GetSet(ctx context.Context, primitiveID runtimev1.PrimitiveId) (Set, error)
}

type Set interface {
	primitive.Primitive
	v1.SetServer
}
