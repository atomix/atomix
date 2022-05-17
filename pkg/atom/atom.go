// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atom

import (
	"context"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

type Registrar[T Atom] func(*grpc.Server, *Service[T], *Registry[T])

func New[T Atom](resolver ClientResolver[T], registrar Registrar[T]) Type {
	return &atomType[T]{
		resolver:  resolver,
		registrar: registrar,
		registry:  NewRegistry[T](),
	}
}

type Type interface {
	Register(server *grpc.Server, connector Connector)
}

type atomType[T Atom] struct {
	resolver  ClientResolver[T]
	registrar Registrar[T]
	registry  *Registry[T]
}

func (a *atomType[T]) Register(server *grpc.Server, connector Connector) {
	a.registrar(server, NewService[T](connector, a.resolver, a.registry), a.registry)
}

type Atom interface {
	Close(ctx context.Context) error
}

type ClientResolver[T Atom] func(client driver.Client) (*Client[T], bool)

// NewClient creates a new client for the given atom type
func NewClient[T Atom](getter func(ctx context.Context, name string) (T, error)) *Client[T] {
	return &Client[T]{
		getter: getter,
	}
}

type Client[T Atom] struct {
	getter func(ctx context.Context, name string) (T, error)
}

func (c *Client[T]) GetAtom(ctx context.Context, name string) (T, error) {
	return c.getter(ctx, name)
}
