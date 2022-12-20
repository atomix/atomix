// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/gogo/protobuf/jsonpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"

	multiraftv1beta1 "github.com/atomix/atomix/stores/multiraft/pkg/apis/multiraft/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	driverName    = "Consensus"
	driverVersion = "v1beta1"
)

func addConsensusStoreController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &MultiRaftStoreReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-multiraft-storage"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-multiraft-store-v1beta1", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &multiraftv1beta1.ConsensusStore{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource MultiRaftCluster
	err = controller.Watch(&source.Kind{Type: &multiraftv1beta1.MultiRaftCluster{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &multiraftv1beta1.ConsensusStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Store
	err = controller.Watch(&source.Kind{Type: &atomixv3beta3.DataStore{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &multiraftv1beta1.MultiRaftCluster{},
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
	log.Info("Reconcile ConsensusStore")
	store := &multiraftv1beta1.ConsensusStore{}
	err := r.client.Get(ctx, request.NamespacedName, store)
	if err != nil {
		log.Error(err, "Reconcile ConsensusStore")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	log.Info("Reconcile raft protocol stateful set")
	cluster := &multiraftv1beta1.MultiRaftCluster{}
	name := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, name, cluster); err != nil && k8serrors.IsNotFound(err) {
		log.Info("Creating MultiRaftCluster", "Name", store.Name, "Namespace", store.Namespace)
		cluster = &multiraftv1beta1.MultiRaftCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:        store.Name,
				Namespace:   store.Namespace,
				Labels:      newClusterLabels(store),
				Annotations: newClusterAnnotations(store),
			},
			Spec: store.Spec.MultiRaftClusterSpec,
		}
		if err := controllerutil.SetControllerReference(store, cluster, r.scheme); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}
		if err := r.client.Create(ctx, cluster); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if cluster.Status.Partitions == nil {
		return reconcile.Result{}, nil
	}

	if cluster.Status.State == multiraftv1beta1.MultiRaftClusterNotReady &&
		store.Status.State != multiraftv1beta1.ConsensusStoreNotReady {
		store.Status.State = multiraftv1beta1.ConsensusStoreNotReady
		if err := r.client.Status().Update(ctx, store); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}

	dataStore := &atomixv3beta3.DataStore{}
	dataStoreName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, dataStoreName, dataStore); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}

		config := getProtocolConfig(cluster.Status.Partitions)
		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err, "Reconcile ConsensusStore")
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
				Config: runtime.RawExtension{
					Raw: []byte(configString),
				},
			},
		}
		if err := controllerutil.SetControllerReference(store, dataStore, r.scheme); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}
		if err := r.client.Create(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	var config rsmv1.ProtocolConfig
	if err := jsonpb.UnmarshalString(string(dataStore.Spec.Config.Raw), &config); err != nil {
		log.Error(err, "Reconcile ConsensusStore")
		return reconcile.Result{}, err
	}

	newConfig := getProtocolConfig(cluster.Status.Partitions)
	if !isProtocolConfigEqual(config, newConfig) {
		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&newConfig)
		if err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}

		dataStore.Spec.Config = runtime.RawExtension{
			Raw: []byte(configString),
		}
		if err := r.client.Update(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if cluster.Status.State == multiraftv1beta1.MultiRaftClusterReady &&
		store.Status.State != multiraftv1beta1.ConsensusStoreReady {
		store.Status.State = multiraftv1beta1.ConsensusStoreReady
		if err := r.client.Status().Update(ctx, store); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}
	return reconcile.Result{}, nil
}

func getProtocolConfig(partitions []multiraftv1beta1.RaftPartitionStatus) rsmv1.ProtocolConfig {
	var config rsmv1.ProtocolConfig
	for _, partition := range partitions {
		var leader string
		if partition.Leader != nil {
			leader = *partition.Leader
		}
		config.Partitions = append(config.Partitions, rsmv1.PartitionConfig{
			PartitionID: rsmv1.PartitionID(partition.PartitionID),
			Leader:      leader,
			Followers:   partition.Followers,
		})
	}
	return config
}

func isProtocolConfigEqual(config1, config2 rsmv1.ProtocolConfig) bool {
	if len(config1.Partitions) != len(config2.Partitions) {
		return false
	}
	for _, partition1 := range config1.Partitions {
		for _, partition2 := range config2.Partitions {
			if partition1.PartitionID == partition2.PartitionID && !isPartitionConfigEqual(partition1, partition2) {
				return false
			}
		}
	}
	return true
}

func isPartitionConfigEqual(partition1, partition2 rsmv1.PartitionConfig) bool {
	if partition1.Leader == "" && partition2.Leader != "" {
		return false
	}
	if partition1.Leader != "" && partition2.Leader == "" {
		return false
	}
	if partition1.Leader != "" && partition2.Leader != "" && partition1.Leader != partition2.Leader {
		return false
	}
	if len(partition1.Followers) != len(partition2.Followers) {
		return false
	}
	for _, follower := range partition1.Followers {
		if !slices.Contains(partition2.Followers, follower) {
			return false
		}
	}
	return true
}

var _ reconcile.Reconciler = (*MultiRaftStoreReconciler)(nil)
