// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
)

type Kind interface {
	Name() string
	Register(server *grpc.Server, registry *Registry)
	GetProxy(client driver.Client) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config *types.Any) (Proxy, error)
}

type Closer interface {
	Close(ctx context.Context) error
}

type Proxy interface {
	Closer
}

type Registrar[T Proxy] func(*grpc.Server, *ProxyManager[T])

type Provider[P any, T Proxy, C proto.Message] func(client P) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, config C) (T, error)

func NewKind[P any, T Proxy, C proto.Message](name string, registrar Registrar[T], provider Provider[P, T, C]) Kind {
	return &genericKind[P, T, C]{
		name:      name,
		registrar: registrar,
		provider:  provider,
	}
}

type genericKind[P any, T Proxy, C proto.Message] struct {
	name      string
	registrar Registrar[T]
	provider  Provider[P, T, C]
}

func (t *genericKind[P, T, C]) Name() string {
	return t.name
}

func (t *genericKind[P, T, C]) Register(server *grpc.Server, registry *Registry) {
	t.registrar(server, newProxyManager[T](registry))
}

func (t *genericKind[P, T, C]) GetProxy(client driver.Client) func(ctx context.Context, primitiveID atomixv1.PrimitiveId, rawConfig *types.Any) (Proxy, error) {
	return func(ctx context.Context, primitiveID atomixv1.PrimitiveId, rawConfig *types.Any) (Proxy, error) {
		if provider, ok := client.(P); ok {
			var config C
			if err := types.UnmarshalAny(rawConfig, config); err != nil {
				return nil, err
			}
			return t.provider(provider)(ctx, primitiveID, config)
		}
		return nil, errors.NewNotSupported("primitive type '%s' not supported by driver", t.name)
	}
}
