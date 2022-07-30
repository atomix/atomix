// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	indexedmapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/indexedmap/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "AtomicIndexedMap"
	APIVersion = "v1"
)

var Type = runtime.NewType[indexedmapv1.AtomicIndexedMapServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[indexedmapv1.AtomicIndexedMapServer]) {
	indexedmapv1.RegisterAtomicIndexedMapServer(server, newAtomicIndexedMapServer(delegate))
}

func resolve(client runtime.Client) (indexedmapv1.AtomicIndexedMapServer, bool) {
	if provider, ok := client.(AtomicIndexedMapProvider); ok {
		return provider.AtomicIndexedMap(), true
	}
	return nil, false
}

type AtomicIndexedMapProvider interface {
	AtomicIndexedMap() indexedmapv1.AtomicIndexedMapServer
}
