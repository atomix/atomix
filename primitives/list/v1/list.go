// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/list/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Primitive = primitive.New[List](clientFactory, func(server *grpc.Server, service *primitive.Service[List], registry *primitive.Registry[List]) {
	v1.RegisterListManagerServer(server, newListV1ManagerServer(service))
	v1.RegisterListServer(server, newListV1Server(registry))
})

// clientFactory is the list/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[List], bool) {
	if listClient, ok := client.(ListClient); ok {
		return primitive.NewClient[List](listClient.GetList), true
	}
	return nil, false
}

type ListClient interface {
	GetList(ctx context.Context, primitiveID runtimev1.PrimitiveId) (List, error)
}

type List interface {
	primitive.Primitive
	v1.ListServer
}
