// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.map.v1.Map"

var Kind = primitive.NewKind[mapv1.MapServer](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[mapv1.MapServer]) {
	mapv1.RegisterMapServer(server, newMapServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[mapv1.MapServer], bool) {
	if _map, ok := client.(MapProvider); ok {
		return _map.GetMap, true
	}
	return nil, false
}

type MapProvider interface {
	GetMap(primitive.ID) mapv1.MapServer
}
