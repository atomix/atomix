// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/atomix/api/pkg/map/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	mapdriverv1 "github.com/atomix/atomix/driver/pkg/driver/map/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.map.v1.Map"

var Type = proxy.NewType[mapv1.MapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[mapv1.MapServer]) {
	mapv1.RegisterMapServer(server, newMapServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (mapv1.MapServer, bool, error) {
	if provider, ok := conn.(mapdriverv1.MapProvider); ok {
		_map, err := provider.NewMap(spec)
		return _map, true, err
	}
	return nil, false, nil
}
