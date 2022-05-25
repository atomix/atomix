// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	listv1 "github.com/atomix/runtime/api/atomix/list/v1"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.list.v1.List"

var Primitive = primitive.NewType[ListClient, List, *listv1.ListConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[List]) {
	listv1.RegisterListServer(server, newListServer(sessions))
}

func create(client ListClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *listv1.ListConfig) (List, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *listv1.ListConfig) (List, error) {
		return client.GetList(ctx, sessionID)
	}
}

type ListClient interface {
	GetList(ctx context.Context, sessionID primitivev1.SessionId) (List, error)
}

type List interface {
	primitive.Primitive
	listv1.ListServer
}
