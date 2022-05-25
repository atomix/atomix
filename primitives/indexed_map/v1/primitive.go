// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	indexedmapv1 "github.com/atomix/runtime/api/atomix/indexed_map/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.indexed_map.v1.IndexedMap"

var Primitive = primitive.NewKind[IndexedMapClient, IndexedMap, *indexedmapv1.IndexedMapConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[IndexedMap]) {
	indexedmapv1.RegisterIndexedMapServer(server, newIndexedMapServer(sessions))
}

func create(client IndexedMapClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *indexedmapv1.IndexedMapConfig) (IndexedMap, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *indexedmapv1.IndexedMapConfig) (IndexedMap, error) {
		return client.GetIndexedMap(ctx, sessionID)
	}
}

type IndexedMapClient interface {
	GetIndexedMap(ctx context.Context, sessionID primitivev1.SessionId) (IndexedMap, error)
}

type IndexedMap interface {
	primitive.Primitive
	indexedmapv1.IndexedMapServer
}
