// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
)

type Type interface {
	Name() string
	Register(server *grpc.Server, registry *SessionRegistry)
	GetProxy(client driver.Client) func(ctx context.Context, sessionID primitivev1.SessionId, rawConfig *types.Any) (Primitive, error)
}

type Closer interface {
	Close(ctx context.Context) error
}

type Primitive interface {
	Closer
}

type Registrar[T Primitive] func(*grpc.Server, *SessionManager[T])

type Provider[P any, T Primitive, C proto.Message] func(client P) func(ctx context.Context, sessionID primitivev1.SessionId, config C) (T, error)

func NewType[P any, T Primitive, C proto.Message](name string, registrar Registrar[T], provider Provider[P, T, C]) Type {
	return &genericType[P, T, C]{
		name:      name,
		registrar: registrar,
		provider:  provider,
	}
}

type genericType[P any, T Primitive, C proto.Message] struct {
	name      string
	registrar Registrar[T]
	provider  Provider[P, T, C]
}

func (t *genericType[P, T, C]) Name() string {
	return t.name
}

func (t *genericType[P, T, C]) Register(server *grpc.Server, registry *SessionRegistry) {
	t.registrar(server, newManager[T](registry))
}

func (t *genericType[P, T, C]) GetProxy(client driver.Client) func(ctx context.Context, sessionID primitivev1.SessionId, rawConfig *types.Any) (Primitive, error) {
	return func(ctx context.Context, sessionID primitivev1.SessionId, rawConfig *types.Any) (Primitive, error) {
		if provider, ok := client.(P); ok {
			var config C
			if err := types.UnmarshalAny(rawConfig, config); err != nil {
				return nil, err
			}
			return t.provider(provider)(ctx, sessionID, config)
		}
		return nil, errors.NewNotSupported("primitive type '%s' not supported by driver", t.name)
	}
}
