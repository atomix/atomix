// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/errors"
)

func newManager[T any](registry *Registry) *Manager[T] {
	return &Manager[T]{
		registry: registry,
	}
}

type Manager[T any] struct {
	registry *Registry
}

func (m *Manager[T]) GetSession(sessionID primitivev1.SessionId) (T, error) {
	session, ok := m.registry.Get(sessionID)
	if !ok {
		return nil, errors.NewNotFound("session '%s' not found", sessionID)
	}
	if proxy, ok := session.(T); ok {
		return proxy, nil
	}
	return nil, errors.NewForbidden("session '%s' is not of the correct type")
}
