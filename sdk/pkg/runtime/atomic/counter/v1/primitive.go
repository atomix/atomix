// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/atomic/counter/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.atomic.counter.v1.AtomicCounter"

var Type = runtime.NewType[counterv1.AtomicCounterServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[counterv1.AtomicCounterServer]) {
	counterv1.RegisterAtomicCounterServer(server, newAtomicCounterServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (counterv1.AtomicCounterServer, error) {
	return conn.AtomicCounter(config)
}
