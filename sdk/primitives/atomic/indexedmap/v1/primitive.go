// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/indexedmap/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.indexedmap.v1.AtomicIndexedMap"

var Type = runtime.NewType[indexedmapv1.AtomicIndexedMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[indexedmapv1.AtomicIndexedMapServer]) {
	indexedmapv1.RegisterAtomicIndexedMapServer(server, newAtomicIndexedMapServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (indexedmapv1.AtomicIndexedMapServer, error) {
	return conn.AtomicIndexedMap(config)
}
