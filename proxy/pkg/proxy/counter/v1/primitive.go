// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/atomix/api/atomix/counter/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	counterdriverv1 "github.com/atomix/atomix/driver/pkg/driver/counter/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.counter.v1.Counter"

var Type = proxy.NewType[counterv1.CounterServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[counterv1.CounterServer]) {
	counterv1.RegisterCounterServer(server, newCounterServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (counterv1.CounterServer, bool, error) {
	if provider, ok := conn.(counterdriverv1.CounterProvider); ok {
		counter, err := provider.NewCounter(spec)
		return counter, true, err
	}
	return nil, false, nil
}
