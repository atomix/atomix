// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/errors"
	"github.com/atomix/runtime/pkg/runtime"
	"google.golang.org/grpc/metadata"
	"sync"
)

const primitiveIDHeader = "Primitive-ID"

func NewManager[T any](primitiveType Type, runtime runtime.Runtime, resolver Resolver[T]) *Manager[T] {
	return &Manager[T]{
		primitiveType: primitiveType,
		runtime:       runtime,
		resolver:      resolver,
		clients:       make(map[string]T),
	}
}

type Manager[T any] struct {
	primitiveType Type
	runtime       runtime.Runtime
	resolver      Resolver[T]
	clients       map[string]T
	mu            sync.RWMutex
}

func (m *Manager[T]) GetClient(ctx context.Context) (T, error) {
	var client T
	name, err := getIDFromContext(ctx)
	if err != nil {
		return client, err
	}

	client, ok := m.getClient(name)
	if ok {
		return client, nil
	}
	return m.newClient(ctx, name)
}

func (m *Manager[T]) getClient(name string) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, ok := m.clients[name]
	return client, ok
}

func (m *Manager[T]) newClient(ctx context.Context, name string) (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[name]
	if ok {
		return client, nil
	}

	conn, err := m.runtime.GetClient(ctx, m.primitiveType.Kind(), name)
	if err != nil {
		return client, err
	}

	provider, ok := m.resolver(conn)
	if !ok {
		return client, errors.NewNotSupported("primitive type not supported by client")
	}

	client = provider(name)
	m.clients[name] = client
	return client, nil
}

func getIDFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.NewInvalid("missing metadata in context")
	}
	primitiveIDs := md.Get(primitiveIDHeader)
	if len(primitiveIDs) == 0 {
		return "", errors.NewInvalid("missing %s header in metadata", primitiveIDHeader)
	}
	return primitiveIDs[0], nil
}
