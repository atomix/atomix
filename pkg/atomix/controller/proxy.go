// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/atomix/runtime"
	"github.com/atomix/runtime/pkg/atomix/store"
)

func newProxyExecutor(runtime runtime.Runtime) *Controller[*runtimev1.ProxyId] {
	proxyWatcher := NewWatcher[*runtimev1.ProxyId, *runtimev1.ProxyId, *runtimev1.Proxy](runtime.Proxies(), func(proxy *runtimev1.Proxy) []*runtimev1.ProxyId {
		return []*runtimev1.ProxyId{&proxy.ID}
	})
	sessionWatcher := NewWatcher[*runtimev1.ProxyId, *runtimev1.SessionId, *runtimev1.Session](runtime.Sessions(), func(session *runtimev1.Session) []*runtimev1.ProxyId {
		proxies := runtime.Proxies().List()
		var proxyIDs []*runtimev1.ProxyId
		for _, proxy := range proxies {
			if proxy.ID.Session == session.ID {
				proxyID := proxy.ID
				proxyIDs = append(proxyIDs, &proxyID)
			}
		}
		return proxyIDs
	})
	return NewController[*runtimev1.ProxyId](newProxyReconciler(runtime), proxyWatcher, sessionWatcher)
}

func newProxyReconciler(runtime runtime.Runtime) Reconciler[*runtimev1.ProxyId] {
	return &proxyReconciler{
		proxies:  runtime.Proxies(),
		sessions: runtime.Sessions(),
	}
}

type proxyReconciler struct {
	proxies  store.Store[*runtimev1.ProxyId, *runtimev1.Proxy]
	sessions store.Store[*runtimev1.SessionId, *runtimev1.Session]
}

func (r *proxyReconciler) Reconcile(proxyID *runtimev1.ProxyId) error {
	//TODO implement me
	panic("implement me")
}

var _ Reconciler[*runtimev1.ProxyId] = (*proxyReconciler)(nil)
