// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.value.v1.Value"

var Kind = primitive.NewKind[valuev1.ValueServer](serviceName, register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[valuev1.ValueServer]) {
	valuev1.RegisterValueServer(server, newValueServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[valuev1.ValueServer], bool) {
	if value, ok := client.(ValueProvider); ok {
		return value.GetValue, true
	}
	return nil, false
}

type ValueProvider interface {
	GetValue(primitive.ID) valuev1.ValueServer
}
