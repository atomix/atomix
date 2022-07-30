// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/map/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "AtomicMap"
	APIVersion = "v1"
)

var Type = runtime.NewType[mapv1.AtomicMapServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[mapv1.AtomicMapServer]) {
	mapv1.RegisterAtomicMapServer(server, newAtomicMapServer(delegate))
}

func resolve(client runtime.Client) (mapv1.AtomicMapServer, bool) {
	if provider, ok := client.(AtomicMapProvider); ok {
		return provider.AtomicMap(), true
	}
	return nil, false
}

type AtomicMapProvider interface {
	AtomicMap() mapv1.AtomicMapServer
}
