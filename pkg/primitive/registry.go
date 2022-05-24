// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	primitivev1 "github.com/atomix/runtime/api/atomix/primitive/v1"
	"sync"
)

func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		sessions: make(map[primitivev1.SessionId]Primitive),
	}
}

type SessionRegistry struct {
	sessions map[primitivev1.SessionId]Primitive
	mu       sync.RWMutex
}

func (r *SessionRegistry) Get(sessionID primitivev1.SessionId) (Primitive, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	primitive, ok := r.sessions[sessionID]
	return primitive, ok
}

func (r *SessionRegistry) Register(sessionID primitivev1.SessionId, primitive Primitive) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[sessionID]; ok {
		return false
	}
	r.sessions[sessionID] = primitive
	return true
}

func (r *SessionRegistry) Unregister(sessionID primitivev1.SessionId) (Primitive, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	primitive, ok := r.sessions[sessionID]
	if !ok {
		return nil, false
	}
	delete(r.sessions, sessionID)
	return primitive, true
}
