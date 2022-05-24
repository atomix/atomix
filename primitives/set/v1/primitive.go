// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	setv1 "github.com/atomix/runtime/api/atomix/primitive/set/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.set.v1.Set"

var Primitive = primitive.NewType[SetClient, Set, *setv1.SetConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[Set]) {
	setv1.RegisterSetServer(server, newSetServer(sessions))
}

func create(client SetClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *setv1.SetConfig) (Set, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *setv1.SetConfig) (Set, error) {
		return client.GetSet(ctx, sessionID)
	}
}

type SetClient interface {
	GetSet(ctx context.Context, sessionID primitivev1.SessionId) (Set, error)
}

type Set interface {
	primitive.Primitive
	setv1.SetServer
}
