// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.list.v1.List"

var Primitive = primitive.NewType[List](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[List], bool) {
	if listClient, ok := client.(ListClient); ok {
		return primitive.NewClient[List](listClient.GetList), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[List], registry *primitive.Registry[List]) {
	listv1.RegisterListServer(server, newListV1Server(registry))
}

type ListClient interface {
	GetList(ctx context.Context, primitiveID runtimev1.ObjectId) (List, error)
}

type List interface {
	primitive.Primitive
	listv1.ListServer
}
