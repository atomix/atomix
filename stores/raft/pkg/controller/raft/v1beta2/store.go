// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/apis/atomix/v3beta3"
	rsmv1 "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/logging"
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

	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	driverName    = "Raft"
	driverVersion = "v2beta1"
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

	// Watch for changes to secondary resource RaftCluster
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftCluster{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
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
	log := log.WithFields(logging.String("RaftStore", request.NamespacedName.String()))
	log.Debug("Reconciling RaftStore")

	store := &raftv1beta2.RaftStore{}
	if ok, err := get(r.client, ctx, request.NamespacedName, store, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}

	cluster := &raftv1beta2.RaftCluster{}
	clusterName := types.NamespacedName{
		Namespace: getClusterNamespace(store, store.Spec.Cluster),
		Name:      store.Spec.Cluster.Name,
	}
	if ok, err := get(r.client, ctx, clusterName, cluster, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcilePartitions(ctx, log, store, cluster); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileDataStore(ctx, log, store, cluster); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *RaftStoreReconciler) reconcilePartitions(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster) (bool, error) {
	if store.Status.ReplicationFactor == nil {
		if store.Spec.ReplicationFactor != nil && *store.Spec.ReplicationFactor <= cluster.Spec.Replicas {
			store.Status.ReplicationFactor = store.Spec.ReplicationFactor
		} else {
			store.Status.ReplicationFactor = &cluster.Spec.Replicas
		}
		if err := updateStatus(r.client, ctx, store, log); err != nil {
			return false, err
		}
		return true, nil
	}

	allReady := true
	for ordinal := 1; ordinal <= int(store.Spec.Partitions); ordinal++ {
		if status, ok, err := r.reconcilePartition(ctx, log, store, cluster, raftv1beta2.PartitionID(ordinal)); err != nil {
			return false, err
		} else if ok {
			return true, nil
		} else if status != raftv1beta2.RaftPartitionReady {
			if store.Status.State != raftv1beta2.RaftStoreNotReady {
				store.Status.State = raftv1beta2.RaftStoreNotReady
				log.Infow("Store status changed", logging.String("Status", string(store.Status.State)))
				if err := updateStatus(r.client, ctx, store, log); err != nil {
					return false, err
				}
				return true, nil
			}
			allReady = false
		}
	}

	if allReady && store.Status.State != raftv1beta2.RaftStoreReady {
		store.Status.State = raftv1beta2.RaftStoreReady
		log.Infow("Store status changed", logging.String("Status", string(store.Status.State)))
		if err := updateStatus(r.client, ctx, store, log); err != nil {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

func (r *RaftStoreReconciler) reconcilePartition(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partitionID raftv1beta2.PartitionID) (raftv1beta2.RaftPartitionState, bool, error) {
	log = log.WithFields(logging.Uint64("PartitionID", uint64(partitionID)))
	partitionName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      fmt.Sprintf("%s-%d", store.Name, partitionID),
	}
	partition := &raftv1beta2.RaftPartition{}
	if ok, err := get(r.client, ctx, partitionName, partition, log); err != nil {
		return "", false, err
	} else if !ok {
		// Allocate new group ID by incrementing the group count in the cluster status
		cluster.Status.Groups++
		log.Debugw("Allocating new group", logging.Uint64("GroupID", uint64(cluster.Status.Groups)))
		if err := updateStatus(r.client, ctx, cluster, log); err != nil {
			return "", false, err
		}

		partition = &raftv1beta2.RaftPartition{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   partitionName.Namespace,
				Name:        partitionName.Name,
				Labels:      newPartitionLabels(cluster, store, partitionID, raftv1beta2.GroupID(cluster.Status.Groups)),
				Annotations: newPartitionAnnotations(cluster, store, partitionID, raftv1beta2.GroupID(cluster.Status.Groups)),
			},
			Spec: raftv1beta2.RaftPartitionSpec{
				RaftConfig:  store.Spec.RaftConfig,
				PartitionID: partitionID,
				GroupID:     raftv1beta2.GroupID(cluster.Status.Groups),
				Replicas:    *store.Status.ReplicationFactor,
			},
		}

		log.Infow("Creating RaftPartition",
			logging.Uint64("RaftPartition", uint64(partition.Spec.GroupID)))
		if err := controllerutil.SetControllerReference(store, partition, r.scheme); err != nil {
			log.Error(err)
			return "", false, err
		}
		if err := controllerutil.SetOwnerReference(cluster, partition, r.scheme); err != nil {
			log.Error(err)
			return "", false, err
		}
		if err := create(r.client, ctx, partition, log); err != nil {
			return "", false, err
		}
		return partition.Status.State, true, nil
	}
	return partition.Status.State, false, nil
}

func (r *RaftStoreReconciler) reconcileDataStore(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster) (bool, error) {
	log.Debug("Reconciling DataStore")
	var config rsmv1.ProtocolConfig
	for ordinal := 1; ordinal <= int(store.Spec.Partitions); ordinal++ {
		partitionName := types.NamespacedName{
			Namespace: store.Namespace,
			Name:      fmt.Sprintf("%s-%d", store.Name, ordinal),
		}
		partition := &raftv1beta2.RaftPartition{}
		if ok, err := get(r.client, ctx, partitionName, partition, log); err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}

		replicaAddresses := make(map[raftv1beta2.ReplicaID]string)
		for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
			replicaID := raftv1beta2.ReplicaID(ordinal)
			replicaAddresses[replicaID] = fmt.Sprintf("%s:%d", getClusterPodDNSName(cluster, getReplicaPodName(cluster, partition, replicaID)), apiPort)
		}

		var leader string
		if partition.Status.Leader != nil {
			if address, ok := replicaAddresses[*partition.Status.Leader]; ok {
				leader = address
				delete(replicaAddresses, *partition.Status.Leader)
			}
		}

		followers := make([]string, 0, len(partition.Status.Followers))
		for _, replicaID := range partition.Status.Followers {
			if address, ok := replicaAddresses[replicaID]; ok {
				followers = append(followers, address)
				delete(replicaAddresses, replicaID)
			}
		}

		for _, address := range replicaAddresses {
			followers = append(followers, address)
		}

		config.Partitions = append(config.Partitions, rsmv1.PartitionConfig{
			PartitionID: rsmv1.PartitionID(partition.Spec.PartitionID),
			Leader:      leader,
			Followers:   followers,
		})
	}

	dataStoreName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	dataStore := &atomixv3beta3.DataStore{}
	if ok, err := get(r.client, ctx, dataStoreName, dataStore, log); err != nil {
		return false, err
	} else if !ok {
		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err)
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
			log.Error(err)
			return false, err
		}
		log.Info("Creating DataStore")
		log.Debugw("Configuring DataStore", logging.String("Config", configString))
		return true, create(r.client, ctx, dataStore, log)
	}

	var storedConfig rsmv1.ProtocolConfig
	if err := jsonpb.UnmarshalString(string(dataStore.Spec.Config.Raw), &storedConfig); err != nil {
		log.Error(err)
		return false, err
	}

	if !isProtocolConfigEqual(storedConfig, config) {
		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err)
			return false, err
		}

		dataStore.Spec.Config = runtime.RawExtension{
			Raw: []byte(configString),
		}
		log.Info("Updating DataStore")
		log.Debugw("Configuring DataStore", logging.String("Config", configString))
		return true, update(r.client, ctx, dataStore, log)
	}
	return false, nil
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

var _ reconcile.Reconciler = (*RaftStoreReconciler)(nil)
