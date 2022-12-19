// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	listv1 "github.com/atomix/atomix/api/pkg/runtime/list/v1"
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "List"
	APIVersion = "v1"
)

var PrimitiveType = runtimev1.PrimitiveType{
	Name:       Name,
	APIVersion: APIVersion,
}

func RegisterServer(server *grpc.Server, rt runtime.Runtime) {
	listv1.RegisterListServer(server, newListServer(runtime.NewPrimitiveClient[List](PrimitiveType, rt, resolve)))
}

func resolve(conn runtime.Conn) (runtime.PrimitiveProvider[List], bool) {
	if provider, ok := conn.(ListProvider); ok {
		return provider.NewList, true
	}
	return nil, false
}

type List listv1.ListServer

type ListProvider interface {
	NewList(config []byte) (List, error)
}
