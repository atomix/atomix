// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	listv1 "github.com/atomix/runtime/api/atomix/runtime/list/v1"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"google.golang.org/grpc"
)

const Service = "atomix.runtime.list.v1.List"

var Type = runtime.NewType[listv1.ListServer](Service, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[listv1.ListServer]) {
	listv1.RegisterListServer(server, newListServer(delegate))
}

func resolve(conn runtime.Conn, config []byte) (listv1.ListServer, error) {
	return conn.List(config)
}
