// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.value.v1.Value"

var Kind = primitive.NewKind[ValueClient, Value, *valuev1.ValueConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[Value]) {
	valuev1.RegisterValueServer(server, newValueServer(proxies))
}

func create(client ValueClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *valuev1.ValueConfig) (Value, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *valuev1.ValueConfig) (Value, error) {
		return client.GetValue(ctx, primitiveID)
	}
}

type ValueClient interface {
	GetValue(ctx context.Context, primitiveID atomixv1.PrimitiveId) (Value, error)
}

type Value interface {
	primitive.Proxy
	valuev1.ValueServer
}
