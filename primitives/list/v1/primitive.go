// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.list.v1.List"

var Kind = primitive.NewKind[ListClient, List, *listv1.ListConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[List]) {
	listv1.RegisterListServer(server, newListServer(proxies))
}

func create(client ListClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *listv1.ListConfig) (List, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *listv1.ListConfig) (List, error) {
		return client.GetList(ctx, primitiveID)
	}
}

type ListClient interface {
	GetList(ctx context.Context, primitiveID atomixv1.PrimitiveId) (List, error)
}

type List interface {
	primitive.Proxy
	listv1.ListServer
}
