// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "List"
	APIVersion = "v1"
)

var Type = primitive.NewType[listv1.ListClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[listv1.ListClient]) {
	listv1.RegisterListServer(server, newListServer(manager))
}

func resolve(client driver.Client) (primitive.Factory[listv1.ListClient], bool) {
	if list, ok := client.(ListProvider); ok {
		return list.GetList, true
	}
	return nil, false
}

type ListProvider interface {
	GetList(string) listv1.ListClient
}
