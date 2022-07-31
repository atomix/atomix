// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valuev1 "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.value.v1.Value"

var Type = runtime.NewType[valuev1.ValueServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[valuev1.ValueServer]) {
	valuev1.RegisterValueServer(server, newValueServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (valuev1.ValueServer, error) {
	return conn.Value(config)
}
