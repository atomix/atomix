// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"github.com/gogo/protobuf/proto"
	"sync"
)

type Provider[I proto.Message, T any] interface {
	Registry() *Registry[I, T]
}

func NewRegistry[I proto.Message, T any]() *Registry[I, T] {
	return &Registry[I, T]{
		objects: make(map[string]T),
	}
}

type Registry[I proto.Message, T any] struct {
	objects map[string]T
	mu      sync.RWMutex
}

func (r *Registry[I, T]) Lookup(id I) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	object, ok := r.objects[id.String()]
	return object, ok
}

func (r *Registry[I, T]) Register(id I, object T) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.objects[id.String()]; ok {
		return false
	}
	r.objects[id.String()] = object
	return true
}

func (r *Registry[I, T]) Unregister(id I) (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	object, ok := r.objects[id.String()]
	if !ok {
		return object, false
	}
	delete(r.objects, id.String())
	return object, true
}
