// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/errors"
	"google.golang.org/grpc/metadata"
	"sync"
)

func NewManager[T any](kind Kind, client Client, resolver Resolver[T]) *Manager[T] {
	return &Manager[T]{
		kind:     kind,
		client:   client,
		resolver: resolver,
		proxies:  make(map[ID]T),
	}
}

type Manager[T any] struct {
	kind     Kind
	client   Client
	resolver Resolver[T]
	proxies  map[ID]T
	mu       sync.RWMutex
}

func (m *Manager[T]) Create(ctx context.Context) (T, error) {
	var proxy T
	id, err := m.getIDFromContext(ctx)
	if err != nil {
		return proxy, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	proxy, ok := m.proxies[id]
	if ok {
		return proxy, nil
	}

	conn, err := m.client.Connect(ctx, id)
	if err != nil {
		return proxy, err
	}

	provider, ok := m.resolver(conn.Client())
	if !ok {
		return proxy, errors.NewNotSupported("primitive type not supported by driver '%s'", conn.Driver())
	}

	proxy = provider(id)
	m.proxies[id] = proxy
	return proxy, nil
}

func (m *Manager[T]) Get(ctx context.Context) (T, error) {
	var proxy T
	id, err := m.getIDFromContext(ctx)
	if err != nil {
		return proxy, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	proxy, ok := m.proxies[id]
	if !ok {
		return proxy, errors.NewForbidden("session not found")
	}
	return proxy, nil
}

func (m *Manager[T]) Close(ctx context.Context) (T, error) {
	var proxy T
	id, err := m.getIDFromContext(ctx)
	if err != nil {
		return proxy, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	proxy, ok := m.proxies[id]
	if ok {
		delete(m.proxies, id)
	}

	if err := m.client.Close(ctx, id); err != nil {
		return proxy, err
	}

	if !ok {
		return proxy, errors.NewForbidden("session not found")
	}
	return proxy, nil
}

func (m *Manager[T]) getProxy(id ID) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	proxy, ok := m.proxies[id]
	return proxy, ok
}

func (m *Manager[T]) getIDFromContext(ctx context.Context) (ID, error) {
	id := ID{Service: m.kind.Service()}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return id, errors.NewInvalid("missing metadata in context")
	}

	primitiveIDs := md.Get(PrimitiveIDHeader)
	if len(primitiveIDs) == 0 {
		return id, errors.NewInvalid("missing %s header in metadata", PrimitiveIDHeader)
	}

	id.Primitive = primitiveIDs[0]

	sessionIDs := md.Get(SessionIDHeader)
	if len(sessionIDs) > 0 {
		id.Session = sessionIDs[0]
	}
	return id, nil
}
