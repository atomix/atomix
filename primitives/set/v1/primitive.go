// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	setv1 "github.com/atomix/runtime/api/atomix/set/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "Set"
	APIVersion = "v1"
)

var Type = primitive.NewType[setv1.SetClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[setv1.SetClient]) {
	setv1.RegisterSetServer(server, newSetServer(manager))
}

func resolve(client runtime.Client) (primitive.Factory[setv1.SetClient], bool) {
	if set, ok := client.(SetProvider); ok {
		return set.GetSet, true
	}
	return nil, false
}

type SetProvider interface {
	GetSet(string) setv1.SetClient
}
