// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

const (
	Name       = "Value"
	APIVersion = "v1"
)

var Type = runtime.NewType[valuev1.ValueServer](Name, APIVersion, register, resolve)

func register(server *grpc.Server, delegate *runtime.Delegate[valuev1.ValueServer]) {
	valuev1.RegisterValueServer(server, newValueServer(delegate))
}

func resolve(client runtime.Client) (valuev1.ValueServer, bool) {
	if provider, ok := client.(ValueProvider); ok {
		return provider.Value(), true
	}
	return nil, false
}

type ValueProvider interface {
	Value() valuev1.ValueServer
}
