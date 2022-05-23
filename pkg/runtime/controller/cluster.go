// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/runtime"
	"github.com/atomix/runtime/pkg/runtime/store"
)

func newClusterExecutor(runtime runtime.Runtime) Executor {
	clusterWatcher := NewWatcher[*runtimev1.ClusterId, *runtimev1.ClusterId, *runtimev1.Cluster](runtime.Clusters(), func(cluster *runtimev1.Cluster) []*runtimev1.ClusterId {
		return []*runtimev1.ClusterId{&cluster.ID}
	})
	return NewExecutor[*runtimev1.ClusterId](newClusterReconciler(runtime), clusterWatcher)
}

func newClusterReconciler(runtime runtime.Runtime) Reconciler[*runtimev1.ClusterId] {
	return &clusterReconciler{
		clusters: runtime.Clusters(),
	}
}

type clusterReconciler struct {
	clusters store.Store[*runtimev1.ClusterId, *runtimev1.Cluster]
}

func (r *clusterReconciler) Reconcile(objectID *runtimev1.ClusterId) error {
	//TODO implement me
	panic("implement me")
}

var _ Reconciler[*runtimev1.ClusterId] = (*clusterReconciler)(nil)
