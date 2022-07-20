// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Map"
	APIVersion = "v1"
)

var Type = runtime.NewType[mapv1.MapServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[mapv1.MapServer]) {
	mapv1.RegisterMapServer(server, newMapServer(delegate))
}

func resolve(client runtime.Client) (mapv1.MapServer, bool) {
	if provider, ok := client.(MapProvider); ok {
		return provider.Map(), true
	}
	return nil, false
}

type MapProvider interface {
	Map() mapv1.MapServer
}
