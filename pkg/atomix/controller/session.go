// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/runtime"
	"github.com/atomix/runtime/pkg/atomix/store"
)

func newSessionExecutor(runtime runtime.Runtime) *Controller[*runtimev1.SessionId] {
	sessionWatcher := NewWatcher[*runtimev1.SessionId, *runtimev1.SessionId, *runtimev1.Session](runtime.Sessions(), func(session *runtimev1.Session) []*runtimev1.SessionId {
		return []*runtimev1.SessionId{&session.ID}
	})
	bindingWatcher := NewWatcher[*runtimev1.SessionId, *runtimev1.BindingId, *runtimev1.Binding](runtime.Bindings(), func(binding *runtimev1.Binding) []*runtimev1.SessionId {
		sessions := runtime.Sessions().List()
		var sessionIDs []*runtimev1.SessionId
		for _, session := range sessions {
			sessionID := session.ID
			sessionIDs = append(sessionIDs, &sessionID)
		}
		return sessionIDs
	})
	return NewController[*runtimev1.SessionId](newSessionReconciler(runtime), sessionWatcher, bindingWatcher)
}

func newSessionReconciler(runtime runtime.Runtime) Reconciler[*runtimev1.SessionId] {
	return &sessionReconciler{
		sessions: runtime.Sessions(),
		bindings: runtime.Bindings(),
	}
}

type sessionReconciler struct {
	sessions store.Store[*runtimev1.SessionId, *runtimev1.Session]
	bindings store.Store[*runtimev1.BindingId, *runtimev1.Binding]
}

func (r *sessionReconciler) Reconcile(sessionID *runtimev1.SessionId) error {
	//TODO implement me
	panic("implement me")
}

var _ Reconciler[*runtimev1.SessionId] = (*sessionReconciler)(nil)
