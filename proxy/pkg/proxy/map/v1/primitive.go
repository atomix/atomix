// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.map.v1.Map"

var Type = proxy.NewType[mapv1.MapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[mapv1.MapServer]) {
	mapv1.RegisterMapServer(server, newMapServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (mapv1.MapServer, bool, error) {
	if provider, ok := conn.(MapProvider); ok {
		_map, err := provider.NewMap(spec)
		return _map, true, err
	}
	return nil, false, nil
}

type MapProvider interface {
	NewMap(spec proxy.PrimitiveSpec) (mapv1.MapServer, error)
}
