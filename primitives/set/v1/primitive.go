// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Set"
	APIVersion = "v1"
)

var Type = runtime.NewType[setv1.SetServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[setv1.SetServer]) {
	setv1.RegisterSetServer(server, newSetServer(delegate))
}

func resolve(client runtime.Client) (setv1.SetServer, bool) {
	if provider, ok := client.(SetProvider); ok {
		return provider.Set(), true
	}
	return nil, false
}

type SetProvider interface {
	Set() setv1.SetServer
}
