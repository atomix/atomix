// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/errors"
	"sync"
)

type Connector func(ctx context.Context, primitive runtimev1.Primitive) (driver.Conn, error)

func NewService[T Primitive](connector Connector, resolver ClientResolver[T], proxies *Registry[T]) *Service[T] {
	return &Service[T]{
		connector: connector,
		resolver:  resolver,
		proxies:   proxies,
		conns:     make(map[string]*Conn[T]),
	}
}

type Service[T Primitive] struct {
	connector Connector
	resolver  ClientResolver[T]
	proxies   *Registry[T]
	conns     map[string]*Conn[T]
	mu        sync.RWMutex
}

func (m *Service[T]) Connect(ctx context.Context, primitive runtimev1.Primitive) (*Conn[T], error) {
	conn, ok := m.getConn(primitive.PrimitiveID)
	if ok {
		return conn, nil
	}
	return m.connect(ctx, primitive)
}

func (m *Service[T]) GetConn(primitiveID runtimev1.PrimitiveId) (*Conn[T], error) {
	conn, ok := m.getConn(primitiveID)
	if !ok {
		return nil, errors.NewUnavailable("connection not found")
	}
	return conn, nil
}

func (m *Service[T]) getConn(primitiveID runtimev1.PrimitiveId) (*Conn[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conn, ok := m.conns[primitiveID.Name]
	return conn, ok
}

func (m *Service[T]) connect(ctx context.Context, primitive runtimev1.Primitive) (*Conn[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cluster, ok := m.conns[primitive.PrimitiveID.Name]
	if ok {
		return cluster, nil
	}

	conn, err := m.connector(ctx, primitive)
	if err != nil {
		return nil, err
	}

	client, ok := m.resolver(conn.Client())
	if !ok {
		return nil, errors.NewNotSupported("primitive type not supported by client for cluster '%s'", cluster)
	}

	cluster = newConn(m.proxies, client)
	m.conns[primitive.PrimitiveID.Name] = cluster
	return cluster, nil
}
