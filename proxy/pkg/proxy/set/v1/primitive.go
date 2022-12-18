// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/atomix/api/pkg/set/v1"
	"github.com/atomix/atomix/driver/pkg/driver"
	setdriverv1 "github.com/atomix/atomix/driver/pkg/driver/set/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.set.v1.Set"

var Type = proxy.NewType[setv1.SetServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[setv1.SetServer]) {
	setv1.RegisterSetServer(server, newSetServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (setv1.SetServer, bool, error) {
	if provider, ok := conn.(setdriverv1.SetProvider); ok {
		set, err := provider.NewSet(spec)
		return set, true, err
	}
	return nil, false, nil
}
