// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	valuev1 "github.com/atomix/atomix/api/pkg/runtime/value/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Value"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	valuev1.RegisterValueServer(server, newValueServer(runtime.NewPrimitiveClient[Value](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Value], bool) {
	if provider, ok := conn.(ValueProvider); ok {
		return provider.NewValue, true
	}
	return nil, false
}

type Value valuev1.ValueServer

type ValueProvider interface {
	NewValue(spec runtimev1.PrimitiveSpec) (Value, error)
}
