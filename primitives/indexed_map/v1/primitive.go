// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "IndexedMap"
	APIVersion = "v1"
)

var Type = runtime.NewType[indexedmapv1.IndexedMapClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[indexedmapv1.IndexedMapClient]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(delegate))
}

func resolve(client runtime.Client) (indexedmapv1.IndexedMapClient, bool) {
	if provider, ok := client.(IndexedMapProvider); ok {
		return provider.IndexedMap(), true
	}
	return nil, false
}

type IndexedMapProvider interface {
	IndexedMap() indexedmapv1.IndexedMapClient
}
