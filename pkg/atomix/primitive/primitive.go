// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/atomix/driver"
	"google.golang.org/grpc"
)

type Kind interface {
	Register(server *grpc.Server, client Client)
}

type Registrar[T any] func(*grpc.Server, *Manager[T])

type Resolver[T any] func(driver.Client) (Factory[T], bool)

type Factory[T any] func(ID) T

func NewKind[T any](registrar Registrar[T], resolver Resolver[T]) Kind {
	return &genericKind[T]{
		registrar: registrar,
		resolver:  resolver,
	}
}

type genericKind[T any] struct {
	registrar Registrar[T]
	resolver  Resolver[T]
}

func (k *genericKind[T]) Register(server *grpc.Server, client Client) {
	k.registrar(server, NewManager[T](client, k.resolver))
}

var _ Kind = (*genericKind[any])(nil)

type Client interface {
	Connect(ctx context.Context, id ID) (driver.Conn, error)
	Close(ctx context.Context, id ID) error
}
