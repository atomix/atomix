// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	"context"
	"github.com/atomix/runtime/pkg/driver"
	"github.com/atomix/runtime/pkg/errors"
	"sync"
)

type Connector func(ctx context.Context, name string) (driver.Conn, error)

func NewService[T Primitive](connector Connector, resolver ClientResolver[T], proxies *Registry[T]) *Service[T] {
	return &Service[T]{
		connector: connector,
		resolver:  resolver,
		proxies:   proxies,
		clusters:  make(map[string]*Cluster[T]),
	}
}

type Service[T Primitive] struct {
	connector Connector
	resolver  ClientResolver[T]
	proxies   *Registry[T]
	clusters  map[string]*Cluster[T]
	mu        sync.RWMutex
}

func (m *Service[T]) GetCluster(ctx context.Context, name string) (*Cluster[T], error) {
	namespace, ok := m.getCluster(name)
	if ok {
		return namespace, nil
	}
	return m.newCluster(ctx, name)
}

func (m *Service[T]) getCluster(name string) (*Cluster[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	namespace, ok := m.clusters[name]
	return namespace, ok
}

func (m *Service[T]) newCluster(ctx context.Context, name string) (*Cluster[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cluster, ok := m.clusters[name]
	if ok {
		return cluster, nil
	}

	conn, err := m.connector(ctx, name)
	if err != nil {
		return nil, err
	}

	client, ok := m.resolver(conn.Client())
	if !ok {
		return nil, errors.NewNotSupported("primitive type not supported by client for cluster '%s'", cluster)
	}

	cluster = newCluster(m.proxies, client)
	m.clusters[name] = cluster
	return cluster, nil
}
