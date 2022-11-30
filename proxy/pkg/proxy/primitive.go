// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"fmt"
	"github.com/atomix/atomix/driver/pkg/driver"
	"google.golang.org/grpc"
)

type PrimitiveSpec = driver.PrimitiveSpec

type Kind struct {
	Name       string
	APIVersion string
}

func (k Kind) String() string {
	return fmt.Sprintf("%s/%s", k.Name, k.APIVersion)
}

type Type interface {
	Service() string
	Register(server *grpc.Server, runtime *Runtime)
}

type Registrar[T any] func(*grpc.Server, *Delegate[T])

type Resolver[T any] func(driver.Conn, driver.PrimitiveSpec) (T, bool, error)

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

func (t *genericType[T]) Register(server *grpc.Server, runtime *Runtime) {
	t.registrar(server, newDelegate[T](t.service, runtime, t.resolver))
}

func (t *genericType[T]) String() string {
	return t.service
}
