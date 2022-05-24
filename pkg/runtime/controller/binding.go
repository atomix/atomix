// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/runtime/pkg/runtime/store"
)

func newBindingExecutor(runtime runtime.Runtime) Executor {
	bindingWatcher := NewWatcher[*runtimev1.BindingId, *runtimev1.BindingId, *runtimev1.Binding](runtime.Bindings(), func(binding *runtimev1.Binding) []*runtimev1.BindingId {
		return []*runtimev1.BindingId{&binding.ID}
	})
	clusterWatcher := NewWatcher[*runtimev1.BindingId, *runtimev1.ClusterId, *runtimev1.Cluster](runtime.Clusters(), func(cluster *runtimev1.Cluster) []*runtimev1.BindingId {
		bindings := runtime.Bindings().List()
		var bindingIDs []*runtimev1.BindingId
		for _, binding := range bindings {
			if binding.Spec.ClusterID == cluster.ID {
				bindingID := binding.ID
				bindingIDs = append(bindingIDs, &bindingID)
			}
		}
		return bindingIDs
	})
	return NewExecutor(newBindingReconciler(runtime), bindingWatcher, clusterWatcher)
}

func newBindingReconciler(runtime runtime.Runtime) Reconciler[*runtimev1.BindingId] {
	return &bindingReconciler{
		bindings: runtime.Bindings(),
		clusters: runtime.Clusters(),
	}
}

type bindingReconciler struct {
	bindings store.Store[*runtimev1.BindingId, *runtimev1.Binding]
	clusters store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]
}

func (r *bindingReconciler) Reconcile(bindingID *runtimev1.BindingId) error {
	//TODO implement me
	panic("implement me")
}

var _ Reconciler[*runtimev1.BindingId] = (*bindingReconciler)(nil)
