// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	valuev1 "github.com/atomix/runtime/api/atomix/value/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "Value"
	APIVersion = "v1"
)

var Type = primitive.NewType[valuev1.ValueClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[valuev1.ValueClient]) {
	valuev1.RegisterValueServer(server, newValueServer(manager))
}

func resolve(client driver.Client) (primitive.Factory[valuev1.ValueClient], bool) {
	if value, ok := client.(ValueProvider); ok {
		return value.GetValue, true
	}
	return nil, false
}

type ValueProvider interface {
	GetValue(string) valuev1.ValueClient
}
