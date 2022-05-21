// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	setv1 "github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.set.v1.Set"

var Primitive = primitive.NewType[Set](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[Set], bool) {
	if setClient, ok := client.(SetClient); ok {
		return primitive.NewClient[Set](setClient.GetSet), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[Set], registry *primitive.Registry[Set]) {
	setv1.RegisterSetServer(server, newSetV1Server(registry))
}

type SetClient interface {
	GetSet(ctx context.Context, primitiveID runtimev1.ObjectId) (Set, error)
}

type Set interface {
	primitive.Primitive
	setv1.SetServer
}
