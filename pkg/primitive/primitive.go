// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/pkg/driver"
	"google.golang.org/grpc"
)

const (
	ApplicationIDHeader = "Application-ID"
	PrimitiveIDHeader   = "Primitive-ID"
	SessionIDHeader     = "Session-ID"
)

type ID struct {
	Service     string
	Application string
	Primitive   string
	Session     string
}

func (i ID) String() string {
	return fmt.Sprintf("%s/%s.%s.%s", i.Service, i.Application, i.Primitive, i.Session)
}

type Kind interface {
	fmt.Stringer
	Service() string
	Register(server *grpc.Server, client Client)
}

type Registrar[T any] func(*grpc.Server, *Manager[T])

type Resolver[T any] func(driver.Client) (Factory[T], bool)

type Factory[T any] func(ID) T

func NewKind[T any](service string, registrar Registrar[T], resolver Resolver[T]) Kind {
	return &genericKind[T]{
		service:   service,
		registrar: registrar,
		resolver:  resolver,
	}
}

type genericKind[T any] struct {
	service   string
	registrar Registrar[T]
	resolver  Resolver[T]
}

func (k *genericKind[T]) Service() string {
	return k.service
}

func (k *genericKind[T]) Register(server *grpc.Server, client Client) {
	k.registrar(server, NewManager[T](k, client, k.resolver))
}

func (k *genericKind[T]) String() string {
	return k.service
}

var _ Kind = (*genericKind[any])(nil)

type Client interface {
	Connect(ctx context.Context, id ID) (driver.Conn, error)
	Close(ctx context.Context, id ID) error
}
