// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.value.v1.Value"

var Primitive = primitive.NewType[Value](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[Value], bool) {
	if valueClient, ok := client.(ValueClient); ok {
		return primitive.NewClient[Value](valueClient.GetValue), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[Value], registry *primitive.Registry[Value]) {
	valuev1.RegisterValueServer(server, newValueV1Server(registry))
}

type ValueClient interface {
	GetValue(ctx context.Context, primitiveID runtimev1.ObjectId) (Value, error)
}

type Value interface {
	primitive.Primitive
	valuev1.ValueServer
}
