// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/pkg/driver"
	countermapv1 "github.com/atomix/atomix/api/pkg/primitive/countermap/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.countermap.v1.CounterMap"

var Type = proxy.NewType[countermapv1.CounterMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[countermapv1.CounterMapServer]) {
	countermapv1.RegisterCounterMapServer(server, newCounterMapServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (countermapv1.CounterMapServer, bool, error) {
	if provider, ok := conn.(countermapv1.CounterMapProvider); ok {
		counterMap, err := provider.NewCounterMap(spec)
		return counterMap, true, err
	}
	return nil, false, nil
}
