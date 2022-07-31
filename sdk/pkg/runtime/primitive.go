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
	Service string
	Name    string
	Tags    map[string]string
}

type Type interface {
	Service() string
	Register(server *grpc.Server, runtime Runtime)
}

type Registrar[T any] func(*grpc.Server, *Delegate[T])

type Resolver[T any] func(Conn, []byte) (T, error)

func NewType[T any](service string, registrar Registrar[T], resolver Resolver[T]) Type {
	return &genericType[T]{
		service:   service,
		registrar: registrar,
		resolver:  resolver,
	}
}

type genericType[T any] struct {
	service   string
	registrar Registrar[T]
	resolver  Resolver[T]
}

func (t *genericType[T]) Service() string {
	return t.service
}

func (t *genericType[T]) Register(server *grpc.Server, runtime Runtime) {
	t.registrar(server, newDelegate[T](t.service, runtime, t.resolver))
}

func (t *genericType[T]) String() string {
	return t.service
}
