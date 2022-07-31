// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.counter.v1.Counter"

var Type = runtime.NewType[counterv1.CounterServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[counterv1.CounterServer]) {
	counterv1.RegisterCounterServer(server, newCounterServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (counterv1.CounterServer, error) {
	return conn.Counter(config)
}
