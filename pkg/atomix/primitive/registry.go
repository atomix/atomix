// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"sync"
)

func NewRegistry() *Registry {
	return &Registry{
		primitives: make(map[atomixv1.PrimitiveId]Proxy),
	}
}

type Registry struct {
	primitives map[atomixv1.PrimitiveId]Proxy
	mu         sync.RWMutex
}

func (r *Registry) Get(primitiveID atomixv1.PrimitiveId) (Proxy, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitive, ok := r.primitives[primitiveID]
	return primitive, ok
}

func (r *Registry) Register(primitiveID atomixv1.PrimitiveId, primitive Proxy) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.primitives[primitiveID]; ok {
		return false
	}
	r.primitives[primitiveID] = primitive
	return true
}

func (r *Registry) Unregister(primitiveID atomixv1.PrimitiveId) (Proxy, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	primitive, ok := r.primitives[primitiveID]
	if !ok {
		return nil, false
	}
	delete(r.primitives, primitiveID)
	return primitive, true
}
