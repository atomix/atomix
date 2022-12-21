// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"

	podmemoryv1beta1 "github.com/atomix/atomix/stores/pod-memory/pkg/apis/podmemory/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	driverName    = "PodMemory"
	driverVersion = "v2beta1"
)

func addPodMemoryStoreController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &MultiRaftStoreReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-pod-memory-storage"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-pod-memory-store-v1beta1", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &podmemoryv1beta1.PodMemoryStore{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Store
	err = controller.Watch(&source.Kind{Type: &atomixv3beta3.DataStore{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &podmemoryv1beta1.PodMemoryStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}
	return nil
}

// MultiRaftStoreReconciler reconciles a MultiRaftCluster object
type MultiRaftStoreReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *MultiRaftStoreReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Info("Reconcile PodMemoryStore")
	store := &podmemoryv1beta1.PodMemoryStore{}
	err := r.client.Get(ctx, request.NamespacedName, store)
	if err != nil {
		log.Error(err, "Reconcile PodMemoryStore")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	dataStore := &atomixv3beta3.DataStore{}
	dataStoreName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, dataStoreName, dataStore); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile PodMemoryStore")
			return reconcile.Result{}, err
		}

		dataStore = &atomixv3beta3.DataStore{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: dataStoreName.Namespace,
				Name:      dataStoreName.Name,
				Labels:    store.Labels,
			},
			Spec: atomixv3beta3.DataStoreSpec{
				Driver: atomixv3beta3.Driver{
					Name:    driverName,
					Version: driverVersion,
				},
			},
		}
		if err := controllerutil.SetControllerReference(store, dataStore, r.scheme); err != nil {
			log.Error(err, "Reconcile PodMemoryStore")
			return reconcile.Result{}, err
		}
		if err := r.client.Create(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile PodMemoryStore")
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if store.Status.State != podmemoryv1beta1.PodMemoryStoreReady {
		store.Status.State = podmemoryv1beta1.PodMemoryStoreReady
		if err := r.client.Status().Update(ctx, store); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}
	return reconcile.Result{}, nil
}

var _ reconcile.Reconciler = (*MultiRaftStoreReconciler)(nil)
