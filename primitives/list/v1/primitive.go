// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

var Kind = primitive.NewKind[listv1.ListServer](register, resolve)

func register(server *grpc.Server, proxies *primitive.Manager[listv1.ListServer]) {
	listv1.RegisterListServer(server, newListServer(proxies))
}

func resolve(client driver.Client) (primitive.Factory[listv1.ListServer], bool) {
	if counter, ok := client.(ListProvider); ok {
		return counter.GetList, true
	}
	return nil, false
}

type ListProvider interface {
	GetList(primitive.ID) listv1.ListServer
}
