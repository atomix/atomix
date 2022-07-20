// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "IndexedMap"
	APIVersion = "v1"
)

var Type = runtime.NewType[indexedmapv1.IndexedMapServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[indexedmapv1.IndexedMapServer]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(delegate))
}

func resolve(client runtime.Client) (indexedmapv1.IndexedMapServer, bool) {
	if provider, ok := client.(IndexedMapProvider); ok {
		return provider.IndexedMap(), true
	}
	return nil, false
}

type IndexedMapProvider interface {
	IndexedMap() indexedmapv1.IndexedMapServer
}
