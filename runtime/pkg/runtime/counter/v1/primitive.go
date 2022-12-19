// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/atomix/api/pkg/runtime/counter/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Counter"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt *runtime.Runtime) {
	counterv1.RegisterCounterServer(server, newCounterServer(runtime.NewPrimitiveClient[Counter](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[Counter], bool) {
	if provider, ok := conn.(CounterProvider); ok {
		return provider.NewCounter, true
	}
	return nil, false
}

type Counter counterv1.CounterServer

type CounterProvider interface {
	NewCounter(config []byte) (Counter, error)
}
