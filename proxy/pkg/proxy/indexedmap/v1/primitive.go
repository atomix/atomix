// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/atomix/api/pkg/indexedmap/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	indexedmapdriverv1 "github.com/atomix/atomix/driver/pkg/driver/indexedmap/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.indexedmap.v1.IndexedMap"

var Type = proxy.NewType[indexedmapv1.IndexedMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[indexedmapv1.IndexedMapServer]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (indexedmapv1.IndexedMapServer, bool, error) {
	if provider, ok := conn.(indexedmapdriverv1.IndexedMapProvider); ok {
		indexedMap, err := provider.NewIndexedMap(spec)
		return indexedMap, true, err
	}
	return nil, false, nil
}
