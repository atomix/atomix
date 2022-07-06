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

const (
	primitiveNamespaceHeader = "Primitive-Namespace"
	primitiveNameHeader      = "Primitive-Name"
)

func NewManager[T any](primitiveType Type, r runtime.Runtime, resolver Resolver[T]) *Manager[T] {
	return &Manager[T]{
		primitiveType: primitiveType,
		runtime:       r,
		resolver:      resolver,
		clients:       make(map[runtime.ID]T),
	}
}

type Manager[T any] struct {
	primitiveType Type
	runtime       runtime.Runtime
	resolver      Resolver[T]
	clients       map[runtime.ID]T
	mu            sync.RWMutex
}

func (m *Manager[T]) GetClient(ctx context.Context) (T, error) {
	var client T
	id, err := getIDFromContext(ctx)
	if err != nil {
		return client, err
	}

	client, ok := m.getClient(id)
	if ok {
		return client, nil
	}
	return m.newClient(ctx, id)
}

func (m *Manager[T]) getClient(id runtime.ID) (T, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, ok := m.clients[id]
	return client, ok
}

func (m *Manager[T]) newClient(ctx context.Context, id runtime.ID) (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[id]
	if ok {
		return client, nil
	}

	conn, err := m.runtime.GetClient(ctx, m.primitiveType.Kind(), id)
	if err != nil {
		return client, err
	}

	provider, ok := m.resolver(conn)
	if !ok {
		return client, errors.NewNotSupported("primitive type not supported by client")
	}

	client = provider(id)
	m.clients[id] = client
	return client, nil
}

func getIDFromContext(ctx context.Context) (runtime.ID, error) {
	var id runtime.ID
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return id, errors.NewInvalid("missing metadata in context")
	}
	primitiveNamespaces := md.Get(primitiveNamespaceHeader)
	if len(primitiveNamespaces) > 0 {
		id.Namespace = primitiveNamespaces[0]
	}
	primitiveNames := md.Get(primitiveNameHeader)
	if len(primitiveNames) == 0 {
		return id, errors.NewInvalid("missing %s header in metadata", primitiveNameHeader)
	}
	return id, nil
}
