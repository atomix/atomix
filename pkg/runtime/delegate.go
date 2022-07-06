// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"github.com/atomix/runtime/pkg/errors"
	"sync"
)

func newDelegate[T any](kind Kind, runtime Runtime, resolver Resolver[T]) *Delegate[T] {
	return &Delegate[T]{
		kind:     kind,
		runtime:  runtime,
		resolver: resolver,
		clients:  make(map[string]T),
	}
}

type Delegate[T any] struct {
	kind     Kind
	runtime  Runtime
	resolver Resolver[T]
	clients  map[string]T
	mu       sync.RWMutex
}

func (p *Delegate[T]) Create(name string, tags map[string]string) (T, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	client, ok := p.clients[name]
	if ok {
		return client, nil
	}

	meta := PrimitiveMeta{
		Name: name,
		Kind: p.kind,
		Tags: tags,
	}

	conn, err := p.runtime.Connect(meta)
	if err != nil {
		return client, err
	}

	client, ok = p.resolver(conn.Client())
	if !ok {
		return client, errors.NewNotSupported("primitive kind '%s' not supported by driver", p.kind)
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
