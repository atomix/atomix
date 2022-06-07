// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

var Kind = primitive.NewKind[indexedmapv1.IndexedMapServer](register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[indexedmapv1.IndexedMapServer]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[indexedmapv1.IndexedMapServer], bool) {
	if counter, ok := client.(IndexedMapProvider); ok {
		return counter.GetIndexedMap, true
	}
	return nil, false
}

type IndexedMapProvider interface {
	GetIndexedMap(primitive.ID) indexedmapv1.IndexedMapServer
}
