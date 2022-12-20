// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/gogo/protobuf/jsonpb"
	corev1 "k8s.io/api/core/v1"
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

	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func addRaftStoreController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &RaftStoreReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-raft"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-raft-store", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftStore{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource DataStore
	err = controller.Watch(&source.Kind{Type: &atomixv3beta3.DataStore{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta2.RaftStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftPartition
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftPartition{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta2.RaftStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource MultiRaftCluster
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.MultiRaftCluster{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		stores := &raftv1beta2.RaftStoreList{}
		if err := mgr.GetClient().List(context.Background(), stores, &client.ListOptions{}); err != nil {
			return nil
		}

		var requests []reconcile.Request
		for _, store := range stores.Items {
			if store.Spec.Cluster.Name == object.GetName() && getClusterNamespace(&store, store.Spec.Cluster) == object.GetNamespace() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: store.Namespace,
						Name:      store.Name,
					},
				})
			}
		}
		return requests
	}))
	if err != nil {
		return err
	}
	return nil
}

// RaftStoreReconciler reconciles a RaftStore object
type RaftStoreReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *RaftStoreReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Info("Reconcile RaftStore")
	store := &raftv1beta2.RaftStore{}
	err := r.client.Get(ctx, request.NamespacedName, store)
	if err != nil {
		log.Error(err, "Reconcile RaftStore")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	cluster := &raftv1beta2.MultiRaftCluster{}
	clusterName := types.NamespacedName{
		Namespace: getClusterNamespace(store, store.Spec.Cluster),
		Name:      store.Spec.Cluster.Name,
	}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile RaftStore")
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcilePartition(ctx, store, cluster); err != nil {
		log.Error(err, "Reconcile RaftStore")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileDataStore(ctx, store, cluster); err != nil {
		log.Error(err, "Reconcile RaftStore")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileStatus(ctx, store); err != nil {
		log.Error(err, "Reconcile RaftStore")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *RaftStoreReconciler) reconcilePartition(ctx context.Context, store *raftv1beta2.RaftStore, cluster *raftv1beta2.MultiRaftCluster) (bool, error) {
	partitionName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      fmt.Sprintf("%s-%d", store.Name, 1),
	}
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, partitionName, partition); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}

		// Lookup the registered shard ID for this partition in the cluster status.
		var shardID *raftv1beta2.ShardID
		for _, partitionStatus := range cluster.Status.PartitionStatuses {
			if partitionStatus.Name == partitionName.Name {
				shardID = &partitionStatus.ShardID
				break
			}
		}

		if shardID == nil {
			cluster.Status.LastShardID++
			cluster.Status.PartitionStatuses = append(cluster.Status.PartitionStatuses, raftv1beta2.MultiRaftClusterPartitionStatus{
				ObjectReference: corev1.ObjectReference{
					Namespace: partitionName.Namespace,
					Name:      partitionName.Name,
				},
				PartitionID: 1,
				ShardID:     cluster.Status.LastShardID,
			})
			if err := r.client.Status().Update(ctx, cluster); err != nil {
				log.Error(err, "Reconcile RaftStore")
				return false, err
			}
			return true, nil
		}

		partition = &raftv1beta2.RaftPartition{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   partitionName.Namespace,
				Name:        partitionName.Name,
				Labels:      newPartitionLabels(cluster, store, 1, *shardID),
				Annotations: newPartitionAnnotations(cluster, store, 1, *shardID),
			},
			Spec: raftv1beta2.RaftPartitionSpec{
				RaftConfig:  store.Spec.RaftConfig,
				Cluster:     store.Spec.Cluster,
				ShardID:     *shardID,
				PartitionID: 1,
				Replicas:    *store.Status.ReplicationFactor,
			},
		}
		if err := controllerutil.SetControllerReference(store, partition, r.scheme); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		if err := controllerutil.SetOwnerReference(cluster, partition, r.scheme); err != nil {
			log.Error(err, "Reconcile MultiRaftStore")
			return false, err
		}
		if err := r.client.Create(ctx, partition); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *RaftStoreReconciler) getPartition(ctx context.Context, store *raftv1beta2.RaftStore) (*raftv1beta2.RaftPartition, error) {
	partitionName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      fmt.Sprintf("%s-%d", store.Name, 1),
	}
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, partitionName, partition); err != nil {
		log.Error(err, "Reconcile RaftStore")
		return nil, err
	}
	return partition, nil
}

func (r *RaftStoreReconciler) reconcileDataStore(ctx context.Context, store *raftv1beta2.RaftStore, cluster *raftv1beta2.MultiRaftCluster) (bool, error) {
	partition, err := r.getPartition(ctx, store)
	if err != nil {
		return false, err
	}

	var leader string
	if partition.Status.Leader != nil {
		memberName := types.NamespacedName{
			Namespace: partition.Namespace,
			Name:      fmt.Sprintf("%s-%d-%d", store.Name, partition.Spec.PartitionID, *partition.Status.Leader),
		}
		member := &raftv1beta2.RaftMember{}
		if err := r.client.Get(ctx, memberName, member); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		leader = fmt.Sprintf("%s:%d", getPodDNSName(cluster, member.Spec.Pod.Name), apiPort)
	}

	followers := make([]string, 0, len(partition.Status.Followers))
	for _, memberID := range partition.Status.Followers {
		memberName := types.NamespacedName{
			Namespace: partition.Namespace,
			Name:      fmt.Sprintf("%s-%d-%d", store.Name, partition.Spec.PartitionID, memberID),
		}
		member := &raftv1beta2.RaftMember{}
		if err := r.client.Get(ctx, memberName, member); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		followers = append(followers, fmt.Sprintf("%s:%d", getPodDNSName(cluster, member.Spec.Pod.Name), apiPort))
	}

	config := rsmv1.ProtocolConfig{
		Partitions: []rsmv1.PartitionConfig{
			{
				PartitionID: rsmv1.PartitionID(partition.Spec.PartitionID),
				Leader:      leader,
				Followers:   followers,
			},
		},
	}

	dataStore := &atomixv3beta3.DataStore{}
	dataStoreName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, dataStoreName, dataStore); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}

		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
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
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		if err := r.client.Create(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		return true, nil
	}

	var storedConfig rsmv1.ProtocolConfig
	if err := jsonpb.UnmarshalString(string(dataStore.Spec.Config.Raw), &storedConfig); err != nil {
		log.Error(err, "Reconcile RaftStore")
		return false, err
	}

	if !isProtocolConfigEqual(storedConfig, config) {
		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}

		dataStore.Spec.Config = runtime.RawExtension{
			Raw: []byte(configString),
		}
		if err := r.client.Update(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *RaftStoreReconciler) reconcileStatus(ctx context.Context, store *raftv1beta2.RaftStore) (bool, error) {
	partition, err := r.getPartition(ctx, store)
	if err != nil {
		return false, err
	}

	state := raftv1beta2.RaftStoreReady
	if partition.Status.State == raftv1beta2.RaftPartitionNotReady {
		state = raftv1beta2.RaftStoreNotReady
	}

	if store.Status.State != state {
		store.Status.State = state
		if err := r.client.Status().Update(ctx, store); err != nil {
			log.Error(err, "Reconcile RaftStore")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

var _ reconcile.Reconciler = (*RaftStoreReconciler)(nil)
