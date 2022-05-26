// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	atomixv1 "github.com/atomix/runtime/api/atomix/v1"
	"github.com/atomix/runtime/pkg/atomix/errors"
)

func newProxyManager[T Proxy](registry *Registry) *ProxyManager[T] {
	return &ProxyManager[T]{
		registry: registry,
	}
}

type ProxyManager[T Proxy] struct {
	registry *Registry
}

func (m *ProxyManager[T]) GetProxy(primitiveID atomixv1.PrimitiveId) (T, error) {
	var t T
	primitive, ok := m.registry.Get(primitiveID)
	if !ok {
		return t, errors.NewNotFound("primitive '%s' not found", primitiveID)
	}
	if t, ok := primitive.(T); ok {
		return t, nil
	}
	return t, errors.NewForbidden("session '%s' is not of the correct type")
}
