// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/runtime/api/atomix/set/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.set.v1.Set"

var Kind = primitive.NewKind[SetClient, Set, *setv1.SetConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[Set]) {
	setv1.RegisterSetServer(server, newSetServer(proxies))
}

func create(client SetClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *setv1.SetConfig) (Set, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *setv1.SetConfig) (Set, error) {
		return client.GetSet(ctx, primitiveID)
	}
}

type SetClient interface {
	GetSet(ctx context.Context, primitiveID atomixv1.PrimitiveId) (Set, error)
}

type Set interface {
	primitive.Proxy
	setv1.SetServer
}
