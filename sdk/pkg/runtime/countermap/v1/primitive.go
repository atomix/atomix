// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	atomiccountermapv1 "github.com/atomix/runtime/api/atomix/runtime/countermap/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.countermap.v1.CounterMap"

var Type = runtime.NewType[atomiccountermapv1.CounterMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[atomiccountermapv1.CounterMapServer]) {
	atomiccountermapv1.RegisterCounterMapServer(server, newCounterMapServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (atomiccountermapv1.CounterMapServer, error) {
	return conn.CounterMap(config)
}
