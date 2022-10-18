// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	"github.com/atomix/runtime/proxy/pkg/proxy"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.set.v1.Set"

var Type = proxy.NewType[setv1.SetServer](Service, register, resolve)

func register(server *grpc.Server, delegate *proxy.Delegate[setv1.SetServer]) {
	setv1.RegisterSetServer(server, newSetServer(delegate))
}

func resolve(conn runtime.Conn, spec proxy.PrimitiveSpec) (setv1.SetServer, bool, error) {
	if provider, ok := conn.(SetProvider); ok {
		set, err := provider.NewSet(spec)
		return set, true, err
	}
	return nil, false, nil
}

type SetProvider interface {
	NewSet(spec proxy.PrimitiveSpec) (setv1.SetServer, error)
}
