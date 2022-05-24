// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	mapv1 "github.com/atomix/runtime/api/atomix/primitive/map/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

const serviceName = "atomix.map.v1.Map"

var Primitive = primitive.NewType[MapClient, Map, *mapv1.MapConfig](serviceName, register, getMap)

func register(server *grpc.Server, proxies *primitive.Manager[Map]) {
	mapv1.RegisterMapServer(server, newProxyingMap(proxies))
}

func getMap(client MapClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *mapv1.MapConfig) (Map, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *mapv1.MapConfig) (Map, error) {
		m, err := client.GetMap(ctx, sessionID)
		if err != nil {
			return nil, err
		}
		// TODO: Construct caching map here
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
