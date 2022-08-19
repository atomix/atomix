// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.map.v1.Map"

var Type = runtime.NewType[mapv1.MapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[mapv1.MapServer]) {
	mapv1.RegisterMapServer(server, newMapServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (mapv1.MapServer, error) {
	return conn.Map(config)
}
