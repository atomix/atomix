// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/atomix/api/pkg/runtime/indexedmap/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "IndexedMap"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(runtime.NewPrimitiveClient[IndexedMap](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[IndexedMap], bool) {
	if provider, ok := conn.(IndexedMapProvider); ok {
		return provider.NewIndexedMap, true
	}
	return nil, false
}

type IndexedMap indexedmapv1.IndexedMapServer

type IndexedMapProvider interface {
	NewIndexedMap(config []byte) (IndexedMap, error)
}
