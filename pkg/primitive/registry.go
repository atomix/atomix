// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"sync"
)

func NewRegistry[T Primitive]() *Registry[T] {
	return &Registry[T]{
		proxies: make(map[runtimev1.PrimitiveId]T),
	}
}

type Registry[T Primitive] struct {
	proxies map[runtimev1.PrimitiveId]T
	mu      sync.RWMutex
}

func (r *Registry[T]) GetProxy(primitiveID runtimev1.PrimitiveId) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	proxy, ok := r.proxies[primitiveID]
	return proxy, ok
}

func (r *Registry[T]) register(primitiveID runtimev1.PrimitiveId, proxy T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proxies[primitiveID] = proxy
}

func (r *Registry[T]) unregister(primitiveID runtimev1.PrimitiveId) (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var proxy T
	proxy, ok := r.proxies[primitiveID]
	if !ok {
		return proxy, false
	}
	delete(r.proxies, primitiveID)
	return proxy, true
}
