// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const (
	Name       = "Map"
	APIVersion = "v1"
)

var Type = primitive.NewType[mapv1.MapClient](Name, APIVersion, register, resolve)

func register(server *grpc.Server, manager *primitive.Manager[mapv1.MapClient]) {
	mapv1.RegisterMapServer(server, newMapServer(manager))
}

func resolve(client runtime.Client) (primitive.Factory[mapv1.MapClient], bool) {
	if _map, ok := client.(MapProvider); ok {
		return _map.GetMap, true
	}
	return nil, false
}

type MapProvider interface {
	GetMap(runtime.ID) mapv1.MapClient
}
