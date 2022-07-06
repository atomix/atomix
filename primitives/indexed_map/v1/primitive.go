// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "IndexedMap"
	APIVersion = "v1"
)

var Type = primitive.NewType[indexedmapv1.IndexedMapClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[indexedmapv1.IndexedMapClient]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(manager))
}

func resolve(client runtime.Client) (primitive.Factory[indexedmapv1.IndexedMapClient], bool) {
	if indexedMap, ok := client.(IndexedMapProvider); ok {
		return indexedMap.GetIndexedMap, true
	}
	return nil, false
}

type IndexedMapProvider interface {
	GetIndexedMap(string) indexedmapv1.IndexedMapClient
}
