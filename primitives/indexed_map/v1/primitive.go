// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.indexed_map.v1.IndexedMap"

var Kind = primitive.NewKind[IndexedMapClient, IndexedMap, *indexedmapv1.IndexedMapConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[IndexedMap]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(proxies))
}

func create(client IndexedMapClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *indexedmapv1.IndexedMapConfig) (IndexedMap, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *indexedmapv1.IndexedMapConfig) (IndexedMap, error) {
		return client.GetIndexedMap(ctx, primitiveID)
	}
}

type IndexedMapClient interface {
	GetIndexedMap(ctx context.Context, primitiveID atomixv1.PrimitiveId) (IndexedMap, error)
}

type IndexedMap interface {
	primitive.Proxy
	indexedmapv1.IndexedMapServer
}
