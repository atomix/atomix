// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	counterv1 "github.com/atomix/runtime/api/atomix/counter/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "Counter"
	APIVersion = "v1"
)

var Type = primitive.NewType[counterv1.CounterClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[counterv1.CounterClient]) {
	counterv1.RegisterCounterServer(server, newCounterServer(manager))
}

func resolve(client driver.Client) (primitive.Factory[counterv1.CounterClient], bool) {
	if counter, ok := client.(CounterProvider); ok {
		return counter.GetCounter, true
	}
	return nil, false
}

type CounterProvider interface {
	GetCounter(string) counterv1.CounterClient
}
