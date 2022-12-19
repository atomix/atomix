// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	multimapv1 "github.com/atomix/atomix/api/pkg/runtime/multimap/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "MultiMap"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	multimapv1.RegisterMultiMapServer(server, newMultiMapServer(runtime.NewPrimitiveClient[MultiMap](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[MultiMap], bool) {
	if provider, ok := conn.(MultiMapProvider); ok {
		return provider.NewMultiMap, true
	}
	return nil, false
}

type MultiMap multimapv1.MultiMapServer

type MultiMapProvider interface {
	NewMultiMap(spec runtimev1.PrimitiveSpec) (MultiMap, error)
}
