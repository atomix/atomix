// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atom

import (
	"sync"
)

func NewRegistry[T Atom]() *Registry[T] {
	return &Registry[T]{
		proxies: make(map[string]T),
	}
}

type Registry[T Atom] struct {
	proxies map[string]T
	mu      sync.RWMutex
}

func (r *Registry[T]) GetProxy(name string) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[name]
	return proxy, ok
}

func (r *Registry[T]) register(name string, proxy T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proxies[name] = proxy
}

func (r *Registry[T]) unregister(name string) (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var proxy T
	proxy, ok := r.proxies[name]
	if !ok {
		return proxy, false
	}
	delete(r.proxies, name)
	return proxy, true
}
