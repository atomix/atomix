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

const serviceName = "atomix.counter.v1.Counter"

var Kind = primitive.NewKind[counterv1.CounterServer](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[counterv1.CounterServer]) {
	counterv1.RegisterCounterServer(server, newCounterServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[counterv1.CounterServer], bool) {
	if counter, ok := client.(CounterProvider); ok {
		return counter.GetCounter, true
	}
	return nil, false
}

type CounterProvider interface {
	GetCounter(primitive.ID) counterv1.CounterServer
}
