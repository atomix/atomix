// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	"google.golang.org/grpc"
)

type Kind struct {
	Name       string
	APIVersion string
}

func (k Kind) String() string {
	return fmt.Sprintf("%s/%s", k.Name, k.APIVersion)
}

type PrimitiveMeta struct {
	Name   string
	Kind   Kind
	Labels map[string]string
}

type Type interface {
	Kind() Kind
	Register(server *grpc.Server, runtime Runtime)
}

type Registrar[T any] func(*grpc.Server, *Delegate[T])

type Resolver[T any] func(Client) (T, bool)

func NewType[T any](name string, version string, registrar Registrar[T], resolver Resolver[T]) Type {
	return &genericType[T]{
		kind: Kind{
			Name:       name,
			APIVersion: version,
		},
		registrar: registrar,
		resolver:  resolver,
	}
}

type genericType[T any] struct {
	kind      Kind
	registrar Registrar[T]
	resolver  Resolver[T]
}

func (t *genericType[T]) Kind() Kind {
	return t.kind
}

func (t *genericType[T]) Register(server *grpc.Server, runtime Runtime) {
	t.registrar(server, newDelegate[T](t.kind, runtime, t.resolver))
}

func (t *genericType[T]) String() string {
	return t.kind.String()
}
