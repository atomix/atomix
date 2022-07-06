// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc"
)

type Type interface {
	Kind() runtime.Kind
	Register(server *grpc.Server, runtime runtime.Runtime)
}

type Registrar[T any] func(*grpc.Server, *Manager[T])

type Resolver[T any] func(runtime.Client) (Factory[T], bool)

type Factory[T any] func(id runtime.ID) T

func NewType[T any](name string, version string, registrar Registrar[T], resolver Resolver[T]) Type {
	return &genericType[T]{
		kind:      runtime.NewKind(name, version),
		registrar: registrar,
		resolver:  resolver,
	}
}

type genericType[T any] struct {
	kind      runtime.Kind
	registrar Registrar[T]
	resolver  Resolver[T]
}

func (t *genericType[T]) Kind() runtime.Kind {
	return t.kind
}

func (t *genericType[T]) Register(server *grpc.Server, runtime runtime.Runtime) {
	t.registrar(server, NewManager[T](t, runtime, t.resolver))
}

var _ Type = (*genericType[any])(nil)
