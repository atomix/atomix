// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	atomiccountermapv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/countermap/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.map.v1.AtomicMap"

var Type = runtime.NewType[atomiccountermapv1.AtomicCounterMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[atomiccountermapv1.AtomicCounterMapServer]) {
	atomiccountermapv1.RegisterAtomicCounterMapServer(server, newAtomicCounterMapServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (atomiccountermapv1.AtomicCounterMapServer, error) {
	return conn.AtomicCounterMap(config)
}
