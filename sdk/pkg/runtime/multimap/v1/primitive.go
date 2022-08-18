// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	multimapv1 "github.com/atomix/runtime/api/atomix/runtime/multimap/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.multimap.v1.MultiMap"

var Type = runtime.NewType[multimapv1.MultiMapServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[multimapv1.MultiMapServer]) {
	multimapv1.RegisterMultiMapServer(server, newMultiMapServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (multimapv1.MultiMapServer, error) {
	return conn.MultiMap(config)
}
