// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/atomix/api/pkg/runtime/map/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Map"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt *runtime.Runtime) {
	mapv1.RegisterMapServer(server, newMapServer(runtime.NewPrimitiveClient[Map](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Map], bool) {
	if provider, ok := conn.(MapProvider); ok {
		return provider.NewMap, true
	}
	return nil, false
}

type Map mapv1.MapServer

type MapProvider interface {
	NewMap(config []byte) (Map, error)
}
