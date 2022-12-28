// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/atomix/api/runtime/set/v1"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Set"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	setv1.RegisterSetServer(server, newSetServer(runtime.NewPrimitiveClient[Set](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Set], bool) {
	if provider, ok := conn.(SetProvider); ok {
		return provider.NewSet, true
	}
	return nil, false
}

type Set setv1.SetServer

type SetProvider interface {
	NewSet(spec runtimev1.Primitive) (Set, error)
}
