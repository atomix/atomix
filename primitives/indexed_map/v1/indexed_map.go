// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.indexed_map.v1.IndexedMap"

var Primitive = primitive.NewType[IndexedMap](serviceName, resolve, register)

func resolve(client driver.Client) (*primitive.Client[IndexedMap], bool) {
	if indexedMapClient, ok := client.(IndexedMapClient); ok {
		return primitive.NewClient[IndexedMap](indexedMapClient.GetIndexedMap), true
	}
	return nil, false
}

func register(server *grpc.Server, service *primitive.Service[IndexedMap], registry *primitive.Registry[IndexedMap]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapV1Server(registry))
}

type IndexedMapClient interface {
	GetIndexedMap(ctx context.Context, primitiveID runtimev1.ObjectId) (IndexedMap, error)
}

type IndexedMap interface {
	primitive.Primitive
	indexedmapv1.IndexedMapServer
}
