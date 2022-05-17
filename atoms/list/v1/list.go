// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/list/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

var Atom = atom.New[List](clientFactory, func(server *grpc.Server, service *atom.Service[List], registry *atom.Registry[List]) {
	v1.RegisterListManagerServer(server, newListV1ManagerServer(service))
	v1.RegisterListServer(server, newListV1Server(registry))
})

// clientFactory is the list/v1 client factory
var clientFactory = func(client driver.Client) (*atom.Client[List], bool) {
	if listClient, ok := client.(ListClient); ok {
		return atom.NewClient[List](listClient.GetList), true
	}
	return nil, false
}

type ListClient interface {
	GetList(ctx context.Context, name string) (List, error)
}

type List interface {
	atom.Atom
	v1.ListServer
}
