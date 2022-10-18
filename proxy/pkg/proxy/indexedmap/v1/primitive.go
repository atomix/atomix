// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.indexedmap.v1.IndexedMap"

var Type = proxy.NewType[indexedmapv1.IndexedMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[indexedmapv1.IndexedMapServer]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (indexedmapv1.IndexedMapServer, bool, error) {
	if provider, ok := conn.(IndexedMapProvider); ok {
		indexedMap, err := provider.NewIndexedMap(spec)
		return indexedMap, true, err
	}
	return nil, false, nil
}

type IndexedMapProvider interface {
	NewIndexedMap(spec proxy.PrimitiveSpec) (indexedmapv1.IndexedMapServer, error)
}
