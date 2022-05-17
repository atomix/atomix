// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

var Atom = atom.New[Set](clientFactory, func(server *grpc.Server, service *atom.Service[Set], registry *atom.Registry[Set]) {
	v1.RegisterSetManagerServer(server, newSetV1ManagerServer(service))
	v1.RegisterSetServer(server, newSetV1Server(registry))
})

// clientFactory is the set/v1 client factory
var clientFactory = func(client driver.Client) (*atom.Client[Set], bool) {
	if setClient, ok := client.(SetClient); ok {
		return atom.NewClient[Set](setClient.GetSet), true
	}
	return nil, false
}

type SetClient interface {
	GetSet(ctx context.Context, name string) (Set, error)
}

type Set interface {
	atom.Atom
	v1.SetServer
}
