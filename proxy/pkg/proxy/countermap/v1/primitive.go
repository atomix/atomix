// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	atomiccountermapv1 "github.com/atomix/atomix/api/atomix/countermap/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	countermapdriverv1 "github.com/atomix/atomix/driver/pkg/driver/countermap/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.countermap.v1.CounterMap"

var Type = proxy.NewType[atomiccountermapv1.CounterMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[atomiccountermapv1.CounterMapServer]) {
	atomiccountermapv1.RegisterCounterMapServer(server, newCounterMapServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (atomiccountermapv1.CounterMapServer, bool, error) {
	if provider, ok := conn.(countermapdriverv1.CounterMapProvider); ok {
		counterMap, err := provider.NewCounterMap(spec)
		return counterMap, true, err
	}
	return nil, false, nil
}
