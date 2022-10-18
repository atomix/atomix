// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	atomiccountermapv1 "github.com/atomix/runtime/api/atomix/runtime/countermap/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.countermap.v1.CounterMap"

var Type = proxy.NewType[atomiccountermapv1.CounterMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[atomiccountermapv1.CounterMapServer]) {
	atomiccountermapv1.RegisterCounterMapServer(server, newCounterMapServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (atomiccountermapv1.CounterMapServer, bool, error) {
	if provider, ok := conn.(CounterMapProvider); ok {
		counterMap, err := provider.NewCounterMap(spec)
		return counterMap, true, err
	}
	return nil, false, nil
}

type CounterMapProvider interface {
	NewCounterMap(spec proxy.PrimitiveSpec) (atomiccountermapv1.CounterMapServer, error)
}
