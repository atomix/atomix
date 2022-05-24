// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/primitive/map/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.map.v1.Map"

var Primitive = primitive.NewType[MapClient, Map, *mapv1.MapConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[Map]) {
	mapv1.RegisterMapServer(server, newMapServer(sessions))
}

func create(client MapClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *mapv1.MapConfig) (Map, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *mapv1.MapConfig) (Map, error) {
		m, err := client.GetMap(ctx, sessionID)
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
	GetMap(ctx context.Context, sessionID primitivev1.SessionId) (Map, error)
}

type Map interface {
	primitive.Primitive
	mapv1.MapServer
}
