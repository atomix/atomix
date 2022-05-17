// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var Atom = primitive.New[IndexedMap](clientFactory, func(server *grpc.Server, service *primitive.Service[IndexedMap], registry *primitive.Registry[IndexedMap]) {
	v1.RegisterIndexedMapManagerServer(server, newIndexedMapV1ManagerServer(service))
	v1.RegisterIndexedMapServer(server, newIndexedMapV1Server(registry))
})

// clientFactory is the indexed_map/v1 client factory
var clientFactory = func(client driver.Client) (*primitive.Client[IndexedMap], bool) {
	if indexedMapClient, ok := client.(IndexedMapClient); ok {
		return primitive.NewClient[IndexedMap](indexedMapClient.GetIndexedMap), true
	}
	return nil, false
}

type IndexedMapClient interface {
	GetIndexedMap(ctx context.Context, name string) (IndexedMap, error)
}

type IndexedMap interface {
	primitive.Primitive
	v1.IndexedMapServer
}
