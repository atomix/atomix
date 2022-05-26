// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/map/v1"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/logging"
	"github.com/atomix/runtime/pkg/atomix/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.map.v1.Map"

var Kind = primitive.NewKind[MapClient, Map, *mapv1.MapConfig](serviceName, register, create)

func register(server *grpc.Server, proxies *primitive.ProxyManager[Map]) {
	mapv1.RegisterMapServer(server, newMapServer(proxies))
}

func create(client MapClient) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *mapv1.MapConfig) (Map, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *mapv1.MapConfig) (Map, error) {
		m, err := client.GetMap(ctx, primitiveID)
		if err != nil {
			return nil, err
		}
		if config.Cache != nil && config.Cache.Enabled {
			m, err = newCachingMap(m, *config.Cache)
			if err != nil {
				return nil, err
			}
		}
		return m, nil
	}
}

type MapClient interface {
	GetMap(ctx context.Context, primitiveID atomixv1.PrimitiveId) (Map, error)
}

type Map interface {
	primitive.Proxy
	mapv1.MapServer
}
