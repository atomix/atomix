// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.indexed_map.v1.IndexedMap"

var Kind = primitive.NewKind[indexedmapv1.IndexedMapClient](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[indexedmapv1.IndexedMapClient]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[indexedmapv1.IndexedMapClient], bool) {
	if indexedMap, ok := client.(IndexedMapProvider); ok {
		return indexedMap.GetIndexedMap, true
	}
	return nil, false
}

type IndexedMapProvider interface {
	GetIndexedMap(primitive.ID) indexedmapv1.IndexedMapClient
}
