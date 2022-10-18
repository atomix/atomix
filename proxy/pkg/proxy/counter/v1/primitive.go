// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.counter.v1.Counter"

var Type = proxy.NewType[counterv1.CounterServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[counterv1.CounterServer]) {
	counterv1.RegisterCounterServer(server, newCounterServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (counterv1.CounterServer, bool, error) {
	if provider, ok := conn.(CounterProvider); ok {
		counter, err := provider.NewCounter(spec)
		return counter, true, err
	}
	return nil, false, nil
}

type CounterProvider interface {
	NewCounter(spec proxy.PrimitiveSpec) (counterv1.CounterServer, error)
}
