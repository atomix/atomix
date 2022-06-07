// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/atomix/errors"
	"sync"
)

func NewManager[T any](client Client, resolver Resolver[T]) *Manager[T] {
	return &Manager[T]{
		client:   client,
		resolver: resolver,
	}
}

type Manager[T any] struct {
	client   Client
	resolver Resolver[T]
	proxies  map[ID]T
	mu       sync.RWMutex
}

func (m *Manager[T]) Connect(ctx context.Context) (T, error) {
	var proxy T
	id, err := getIDFromContext(ctx)
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
	id, err := getIDFromContext(ctx)
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
	id, err := getIDFromContext(ctx)
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
