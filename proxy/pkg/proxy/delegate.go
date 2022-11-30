// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"github.com/atomix/atomix/driver/pkg/driver"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"sync"
)

func newDelegate[T any](service string, runtime *Runtime, resolver Resolver[T]) *Delegate[T] {
	return &Delegate[T]{
		service:  service,
		runtime:  runtime,
		resolver: resolver,
		clients:  make(map[string]T),
	}
}

type Delegate[T any] struct {
	service  string
	runtime  *Runtime
	resolver Resolver[T]
	clients  map[string]T
	mu       sync.RWMutex
}

func (p *Delegate[T]) Create(name string, tags []string) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	client, ok := p.clients[name]
	if ok {
		return client, nil
	}

	context := routerContext{
		service: p.service,
		name:    name,
		tags:    tags,
	}

	conn, config, err := p.runtime.getConn(context)
	if err != nil {
		return client, err
	}

	spec := driver.PrimitiveSpec{
		Name:      name,
		Namespace: getNamespace(),
		Service:   p.service,
		Config:    config,
	}

	client, ok, err = p.resolver(conn, spec)
	if !ok {
		return client, errors.NewNotSupported("%s not supported by driver", p.service)
	}
	if err != nil {
		return client, err
	}

	p.clients[name] = client
	return client, nil
}

func (p *Delegate[T]) Get(name string) (T, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	client, ok := p.clients[name]
	if !ok {
		return client, errors.NewForbidden("client not found for '%s'", name)
	}
	return client, nil
}

func (p *Delegate[T]) Close(name string) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	client, ok := p.clients[name]
	if !ok {
		return client, errors.NewForbidden("client not found for '%s'", name)
	}
	delete(p.clients, name)
	return client, nil
}
