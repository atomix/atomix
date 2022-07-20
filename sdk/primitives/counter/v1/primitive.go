// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Counter"
	APIVersion = "v1"
)

var Type = runtime.NewType[counterv1.CounterServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[counterv1.CounterServer]) {
	counterv1.RegisterCounterServer(server, newCounterServer(delegate))
}

func resolve(client runtime.Client) (counterv1.CounterServer, bool) {
	if counter, ok := client.(CounterProvider); ok {
		return counter.Counter(), true
	}
	return nil, false
}

type CounterProvider interface {
	Counter() counterv1.CounterServer
}
