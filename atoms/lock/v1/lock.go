// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	"github.com/atomix/runtime/api/atomix/lock/v1"
	"github.com/atomix/runtime/pkg/atom"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

var Atom = atom.New[Lock](clientFactory, func(server *grpc.Server, service *atom.Service[Lock], registry *atom.Registry[Lock]) {
	v1.RegisterLockManagerServer(server, newLockV1ManagerServer(service))
	v1.RegisterLockServer(server, newLockV1Server(registry))
})

// clientFactory is the lock/v1 client factory
var clientFactory = func(client driver.Client) (*atom.Client[Lock], bool) {
	if lockClient, ok := client.(LockClient); ok {
		return atom.NewClient[Lock](lockClient.GetLock), true
	}
	return nil, false
}

type LockClient interface {
	GetLock(ctx context.Context, name string) (Lock, error)
}

type Lock interface {
	atom.Atom
	v1.LockServer
}
