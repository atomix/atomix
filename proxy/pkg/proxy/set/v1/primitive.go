// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"github.com/atomix/atomix/api/pkg/driver"
	setv1 "github.com/atomix/atomix/api/pkg/primitive/set/v1"
	"github.com/atomix/atomix/proxy/pkg/proxy"
	"google.golang.org/grpc"
)

const Service = "atomix.set.v1.Set"

var Type = proxy.NewType[setv1.SetServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[setv1.SetServer]) {
	setv1.RegisterSetServer(server, newSetServer(delegate))
}

func resolve(conn driver.Conn, spec proxy.PrimitiveSpec) (setv1.SetServer, bool, error) {
	if provider, ok := conn.(setv1.SetProvider); ok {
		set, err := provider.NewSet(spec)
		return set, true, err
	}
	return nil, false, nil
}
