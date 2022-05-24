// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"github.com/atomix/runtime/pkg/errors"
)

func newManager[T any](registry *SessionRegistry) *SessionManager[T] {
	return &SessionManager[T]{
		registry: registry,
	}
}

type SessionManager[T any] struct {
	registry *SessionRegistry
}

func (m *SessionManager[T]) GetSession(sessionID primitivev1.SessionId) (T, error) {
	var session T
	primitive, ok := m.registry.Get(sessionID)
	if !ok {
		return session, errors.NewNotFound("session '%s' not found", sessionID)
	}
	if session, ok := primitive.(T); ok {
		return session, nil
	}
	return session, errors.NewForbidden("session '%s' is not of the correct type")
}
