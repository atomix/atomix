// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

type Registrar[T Primitive] func(*grpc.Server, *Service[T], *Registry[T])

func NewType[T Primitive](name string, resolver ClientResolver[T], registrar Registrar[T]) Type {
	return &primitiveType[T]{
		name:      name,
		resolver:  resolver,
		registrar: registrar,
		registry:  NewRegistry[T](),
	}
}

type Type interface {
	Name() string
	Register(server *grpc.Server, connector Connector)
}

type primitiveType[T Primitive] struct {
	name      string
	resolver  ClientResolver[T]
	registrar Registrar[T]
	registry  *Registry[T]
}

func (t *primitiveType[T]) Name() string {
	return t.name
}

func (t *primitiveType[T]) Register(server *grpc.Server, connector Connector) {
	t.registrar(server, NewService[T](connector, t.resolver, t.registry), t.registry)
}

type Primitive interface {
	Close(ctx context.Context) error
}

type ClientResolver[T Primitive] func(client driver.Client) (*Client[T], bool)

type Provider[T Primitive] func(ctx context.Context, primitiveID runtimev1.ObjectId) (T, error)

// NewClient creates a new client for the given primitive type
func NewClient[T Primitive](provider Provider[T]) *Client[T] {
	return &Client[T]{
		provider: provider,
	}
}

type Client[T Primitive] struct {
	provider func(ctx context.Context, primitiveID runtimev1.ObjectId) (T, error)
}

func (c *Client[T]) GetPrimitive(ctx context.Context, primitiveID runtimev1.ObjectId) (T, error) {
	return c.provider(ctx, primitiveID)
}
