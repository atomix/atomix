// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/runtime/pkg/runtime/store"
)

func newProxyExecutor(runtime runtime.Runtime) Executor {
	proxyWatcher := NewWatcher[*runtimev1.ProxyId, *runtimev1.ProxyId, *runtimev1.Proxy](runtime.Proxies(), func(proxy *runtimev1.Proxy) []*runtimev1.ProxyId {
		return []*runtimev1.ProxyId{&proxy.ID}
	})
	bindingWatcher := NewWatcher[*runtimev1.ProxyId, *runtimev1.BindingId, *runtimev1.Binding](runtime.Bindings(), func(binding *runtimev1.Binding) []*runtimev1.ProxyId {
		proxies := runtime.Proxies().List()
		var proxyIDs []*runtimev1.ProxyId
		for _, proxy := range proxies {
			proxyID := proxy.ID
			proxyIDs = append(proxyIDs, &proxyID)
		}
		return proxyIDs
	})
	return NewExecutor[*runtimev1.ProxyId](newProxyReconciler(runtime), proxyWatcher, bindingWatcher)
}

func newProxyReconciler(runtime runtime.Runtime) Reconciler[*runtimev1.ProxyId] {
	return &proxyReconciler{
		proxies:  runtime.Proxies(),
		bindings: runtime.Bindings(),
	}
}

type proxyReconciler struct {
	proxies  store.Store[*runtimev1.ProxyId, *runtimev1.Proxy]
	bindings store.Store[*runtimev1.BindingId, *runtimev1.Binding]
}

func (r *proxyReconciler) Reconcile(proxyID *runtimev1.ProxyId) error {
	//TODO implement me
	panic("implement me")
}

var _ Reconciler[*runtimev1.ProxyId] = (*proxyReconciler)(nil)
