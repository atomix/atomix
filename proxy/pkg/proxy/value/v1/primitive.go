// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	valuedriverv1 "github.com/atomix/atomix/driver/pkg/driver/value/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	valuev1 "github.com/atomix/atomix/runtime/api/atomix/runtime/value/v1"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.value.v1.Value"

var Type = proxy.NewType[valuev1.ValueServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[valuev1.ValueServer]) {
	valuev1.RegisterValueServer(server, newValueServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (valuev1.ValueServer, bool, error) {
	if provider, ok := conn.(valuedriverv1.ValueProvider); ok {
		value, err := provider.NewValue(spec)
		return value, true, err
	}
	return nil, false, nil
}
