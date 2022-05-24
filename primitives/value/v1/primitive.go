// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	"context"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	valuev1 "github.com/atomix/runtime/api/atomix/primitive/value/v1"
	"github.com/atomix/runtime/pkg/logging"
	"github.com/atomix/runtime/pkg/primitive"
	"google.golang.org/grpc"
)

var log = logging.GetLogger()

const serviceName = "atomix.value.v1.Value"

var Primitive = primitive.NewType[ValueClient, Value, *valuev1.ValueConfig](serviceName, register, create)

func register(server *grpc.Server, sessions *primitive.SessionManager[Value]) {
	valuev1.RegisterValueServer(server, newValueServer(sessions))
}

func create(client ValueClient) func(ctx context.Context, sessionID primitivev1.SessionId, config *valuev1.ValueConfig) (Value, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, config *valuev1.ValueConfig) (Value, error) {
		return client.GetValue(ctx, sessionID)
	}
}

type ValueClient interface {
	GetValue(ctx context.Context, sessionID primitivev1.SessionId) (Value, error)
}

type Value interface {
	primitive.Primitive
	valuev1.ValueServer
}
