// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	countermapv1 "github.com/atomix/atomix/api/pkg/runtime/countermap/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "CounterMap"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	countermapv1.RegisterCounterMapServer(server, newCounterMapServer(runtime.NewPrimitiveClient[CounterMap](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[CounterMap], bool) {
	if provider, ok := conn.(CounterMapProvider); ok {
		return provider.NewCounterMap, true
	}
	return nil, false
}

type CounterMap countermapv1.CounterMapServer

type CounterMapProvider interface {
	NewCounterMap(config []byte) (CounterMap, error)
}
