// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valuev1 "github.com/atomix/runtime/api/atomix/runtime/atomic/value/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.value.v1.AtomicValue"

var Type = runtime.NewType[valuev1.AtomicValueServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[valuev1.AtomicValueServer]) {
	valuev1.RegisterAtomicValueServer(server, newAtomicValueServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (valuev1.AtomicValueServer, error) {
	return conn.AtomicValue(config)
}
