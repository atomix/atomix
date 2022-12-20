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
	"k8s.io/utils/strings/slices"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
	"time"

	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	driverName    = "MultiRaft"
	driverVersion = "v1beta2"
)

func addMultiRaftStoreController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &MultiRaftStoreReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-consensus-storage"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-raft-store", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.MultiRaftStore{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource DataStore
	err = controller.Watch(&source.Kind{Type: &atomixv3beta3.DataStore{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta2.MultiRaftStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftPartition
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftPartition{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta2.MultiRaftStore{},
		IsController: true,
	})
	if err != nil {
		return err
	}
	return nil
}

// MultiRaftStoreReconciler reconciles a MultiRaftStore object
type MultiRaftStoreReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *MultiRaftStoreReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log.Info("Reconcile MultiRaftStore")
	store := &raftv1beta2.MultiRaftStore{}
	err := r.client.Get(ctx, request.NamespacedName, store)
	if err != nil {
		log.Error(err, "Reconcile MultiRaftStore")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if ok, err := r.reconcilePartitions(ctx, store); err != nil {
		log.Error(err, "Reconcile MultiRaftStore")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileDataStore(ctx, store); err != nil {
		log.Error(err, "Reconcile MultiRaftStore")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileStatus(ctx, store); err != nil {
		log.Error(err, "Reconcile MultiRaftStore")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *MultiRaftStoreReconciler) reconcilePartitions(ctx context.Context, store *raftv1beta2.MultiRaftStore) (bool, error) {
	cluster := &raftv1beta2.MultiRaftCluster{}
	clusterName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Spec.Cluster.Name,
	}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile MultiRaftStore")
			return false, err
		}
		return true, nil
	}

	if store.Status.ReplicationFactor == nil {
		if store.Spec.ReplicationFactor != nil && *store.Spec.ReplicationFactor <= cluster.Spec.Replicas {
			store.Status.ReplicationFactor = store.Spec.ReplicationFactor
		} else {
			store.Status.ReplicationFactor = &cluster.Spec.Replicas
		}
		if err := r.client.Status().Update(ctx, store); err != nil {
			log.Error(err, "Reconcile MultiRaftStore")
			return false, err
		}
		return true, nil
	}

	for ordinal := 1; ordinal <= int(store.Spec.Partitions); ordinal++ {
		if updated, err := r.reconcilePartition(ctx, store, cluster, raftv1beta2.PartitionID(ordinal)); err != nil {
			return false, err
		} else if updated {
			return true, nil
		}
	}
	return false, nil
}

func (r *MultiRaftStoreReconciler) reconcilePartition(ctx context.Context, store *raftv1beta2.MultiRaftStore, cluster *raftv1beta2.MultiRaftCluster, partitionID raftv1beta2.PartitionID) (bool, error) {
	partitionName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      fmt.Sprintf("%s-%d", store.Name, partitionID),
	}
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, partitionName, partition); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile MultiRaftStore")
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
				LocalObjectReference: corev1.LocalObjectReference{
					Name: partitionName.Name,
				},
				PartitionID: partitionID,
				ShardID:     cluster.Status.LastShardID,
			})
			if err := r.client.Status().Update(ctx, cluster); err != nil {
				log.Error(err, "Reconcile MultiRaftStore")
				return false, err
			}
			return true, nil
		}

		partition = &raftv1beta2.RaftPartition{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   partitionName.Namespace,
				Name:        partitionName.Name,
				Labels:      newPartitionLabels(store, partitionID, *shardID),
				Annotations: newPartitionAnnotations(store, partitionID, *shardID),
			},
			Spec: raftv1beta2.RaftPartitionSpec{
				RaftConfig:  store.Spec.RaftConfig,
				Cluster:     store.Spec.Cluster,
				ShardID:     *shardID,
				PartitionID: partitionID,
				Replicas:    *store.Status.ReplicationFactor,
			},
		}
		if err := controllerutil.SetControllerReference(store, partition, r.scheme); err != nil {
			log.Error(err, "Reconcile MultiRaftStore")
			return false, err
		}
		if err := r.client.Create(ctx, partition); err != nil {
			log.Error(err, "Reconcile MultiRaftStore")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *MultiRaftStoreReconciler) getPartitions(ctx context.Context, store *raftv1beta2.MultiRaftStore) ([]*raftv1beta2.RaftPartition, error) {
	var partitions []*raftv1beta2.RaftPartition
	for ordinal := 1; ordinal <= int(store.Spec.Partitions); ordinal++ {
		partitionName := types.NamespacedName{
			Namespace: store.Namespace,
			Name:      fmt.Sprintf("%s-%d", store.Name, ordinal),
		}
		partition := &raftv1beta2.RaftPartition{}
		if err := r.client.Get(ctx, partitionName, partition); err != nil {
			log.Error(err, "Reconcile MultiRaftStore")
			return nil, err
		}
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

func (r *MultiRaftStoreReconciler) reconcileDataStore(ctx context.Context, store *raftv1beta2.MultiRaftStore) (bool, error) {
	partitions, err := r.getPartitions(ctx, store)
	if err != nil {
		return false, err
	}

	var config rsmv1.ProtocolConfig
	for _, partition := range partitions {
		var leader string
		if partition.Status.Leader != nil {
			memberName := types.NamespacedName{
				Namespace: partition.Namespace,
				Name:      fmt.Sprintf("%s-%d-%d", store.Name, partition.Spec.PartitionID, *partition.Status.Leader),
			}
			member := &raftv1beta2.RaftMember{}
			if err := r.client.Get(ctx, memberName, member); err != nil {
				log.Error(err, "Reconcile MultiRaftStore")
				return false, err
			}
			leader = fmt.Sprintf("%s:%d", getPodDNSName(store.Namespace, store.Spec.Cluster.Name, member.Spec.Pod.Name), apiPort)
		}

		followers := make([]string, 0, len(partition.Status.Followers))
		for _, memberID := range partition.Status.Followers {
			memberName := types.NamespacedName{
				Namespace: partition.Namespace,
				Name:      fmt.Sprintf("%s-%d-%d", store.Name, partition.Spec.PartitionID, memberID),
			}
			member := &raftv1beta2.RaftMember{}
			if err := r.client.Get(ctx, memberName, member); err != nil {
				log.Error(err, "Reconcile MultiRaftStore")
				return false, err
			}
			followers = append(followers, fmt.Sprintf("%s:%d", getPodDNSName(store.Namespace, store.Spec.Cluster.Name, member.Spec.Pod.Name), apiPort))
		}

		config.Partitions = append(config.Partitions, rsmv1.PartitionConfig{
			PartitionID: rsmv1.PartitionID(partition.Spec.PartitionID),
			Leader:      leader,
			Followers:   followers,
		})
	}

	dataStore := &atomixv3beta3.DataStore{}
	dataStoreName := types.NamespacedName{
		Namespace: store.Namespace,
		Name:      store.Name,
	}
	if err := r.client.Get(ctx, dataStoreName, dataStore); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile ConsensusStore")
			return false, err
		}

		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err, "Reconcile ConsensusStore")
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
			log.Error(err, "Reconcile ConsensusStore")
			return false, err
		}
		if err := r.client.Create(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return false, err
		}
		return true, nil
	}

	var storedConfig rsmv1.ProtocolConfig
	if err := jsonpb.UnmarshalString(string(dataStore.Spec.Config.Raw), &storedConfig); err != nil {
		log.Error(err, "Reconcile ConsensusStore")
		return false, err
	}

	if !isProtocolConfigEqual(storedConfig, config) {
		marshaler := &jsonpb.Marshaler{}
		configString, err := marshaler.MarshalToString(&config)
		if err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return false, err
		}

		dataStore.Spec.Config = runtime.RawExtension{
			Raw: []byte(configString),
		}
		if err := r.client.Update(ctx, dataStore); err != nil {
			log.Error(err, "Reconcile ConsensusStore")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *MultiRaftStoreReconciler) reconcileStatus(ctx context.Context, store *raftv1beta2.MultiRaftStore) (bool, error) {
	partitions, err := r.getPartitions(ctx, store)
	if err != nil {
		return false, err
	}

	state := raftv1beta2.MultiRaftStoreReady
	for _, partition := range partitions {
		if partition.Status.State == raftv1beta2.RaftPartitionNotReady {
			state = raftv1beta2.MultiRaftStoreNotReady
		}
	}

	if store.Status.State != state {
		store.Status.State = state
		if err := r.client.Status().Update(ctx, store); err != nil {
			log.Error(err, "Reconcile MultiRaftStore")
			return false, err
		}
		return true, nil
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

var _ reconcile.Reconciler = (*MultiRaftStoreReconciler)(nil)

// newPartitionLabels returns the labels for the given partition
func newPartitionLabels(store *raftv1beta2.MultiRaftStore, partitionID raftv1beta2.PartitionID, shardID raftv1beta2.ShardID) map[string]string {
	labels := make(map[string]string)
	for key, value := range store.Labels {
		labels[key] = value
	}
	labels[storeKey] = store.Name
	labels[raftStoreKey] = store.Name
	labels[raftClusterKey] = store.Spec.Cluster.Name
	labels[raftPartitionKey] = strconv.Itoa(int(partitionID))
	labels[raftShardKey] = strconv.Itoa(int(shardID))
	return labels
}

func newPartitionAnnotations(store *raftv1beta2.MultiRaftStore, partitionID raftv1beta2.PartitionID, shardID raftv1beta2.ShardID) map[string]string {
	annotations := make(map[string]string)
	for key, value := range store.Annotations {
		annotations[key] = value
	}
	annotations[storeKey] = store.Name
	annotations[raftStoreKey] = store.Name
	annotations[raftClusterKey] = store.Spec.Cluster.Name
	annotations[raftPartitionKey] = strconv.Itoa(int(partitionID))
	annotations[raftShardKey] = strconv.Itoa(int(shardID))
	return annotations
}
