// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	raftv1 "github.com/atomix/atomix/stores/raft/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sync"
	"time"

	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func addRaftMemberController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &RaftMemberReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-raft"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-raft-member", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftMember{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Pod
	err = controller.Watch(&source.Kind{Type: &corev1.Pod{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		members := &raftv1beta2.RaftMemberList{}
		if err := mgr.GetClient().List(context.Background(), members, &client.ListOptions{Namespace: object.GetNamespace()}); err != nil {
			return nil
		}

		var requests []reconcile.Request
		for _, member := range members.Items {
			if member.Spec.Pod.Name == object.GetName() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: object.GetNamespace(),
						Name:      member.Name,
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

// RaftMemberReconciler reconciles a RaftMember object
type RaftMemberReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
	mu     sync.RWMutex
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *RaftMemberReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("RaftMember", request.NamespacedName.String()))
	log.Debug("Reconciling RaftMember")

	member := &raftv1beta2.RaftMember{}
	err := r.client.Get(ctx, request.NamespacedName, member)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Error(err)
		return reconcile.Result{}, err
	}

	if member.DeletionTimestamp != nil {
		if err := r.reconcileDelete(ctx, log, member); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if err := r.reconcileCreate(ctx, log, member); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *RaftMemberReconciler) reconcileCreate(ctx context.Context, log logging.Logger, member *raftv1beta2.RaftMember) error {
	if !hasFinalizer(member, raftMemberKey) {
		log.Debugf("Adding %s finalizer", raftMemberKey)
		addFinalizer(member, raftMemberKey)
		if err := r.client.Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return err
		}
		return nil
	}

	storeName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Annotations[raftStoreKey],
	}
	store := &raftv1beta2.RaftStore{}
	if err := r.client.Get(ctx, storeName, store); err != nil {
		log.Warn(err)
		return err
	}

	clusterName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Spec.Cluster.Name,
	}
	cluster := &raftv1beta2.RaftCluster{}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		log.Warn(err)
		return err
	}

	partitionName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      fmt.Sprintf("%s-%s", storeName.Name, member.Annotations[raftPartitionKey]),
	}
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, partitionName, partition); err != nil {
		log.Warn(err)
		return err
	}

	podName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Spec.Pod.Name,
	}
	pod := &corev1.Pod{}
	if err := r.client.Get(ctx, podName, pod); err != nil {
		log.Warn(err)
		return err
	}

	log = log.WithFields(
		logging.Uint64("PartitionID", uint64(partition.Spec.PartitionID)),
		logging.Uint64("PartitionID", uint64(partition.Spec.ShardID)),
		logging.Uint64("MemberID", uint64(member.Spec.MemberID)),
		logging.Uint64("ReplicaID", uint64(member.Spec.ReplicaID)),
		logging.Stringer("Pod", podName))
	if ok, err := r.addMember(ctx, log, store, cluster, partition, pod, member); err != nil {
		return err
	} else if ok {
		return nil
	}
	return nil
}

func (r *RaftMemberReconciler) reconcileDelete(ctx context.Context, log logging.Logger, member *raftv1beta2.RaftMember) error {
	if !hasFinalizer(member, raftMemberKey) {
		return nil
	}

	storeName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Annotations[raftStoreKey],
	}
	store := &raftv1beta2.RaftStore{}
	if err := r.client.Get(ctx, storeName, store); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debugf("Removing %s finalizer", raftMemberKey)
		removeFinalizer(member, raftMemberKey)
		if err := r.client.Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return err
		}
		return nil
	}

	clusterName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Spec.Cluster.Name,
	}
	cluster := &raftv1beta2.RaftCluster{}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debugf("Removing %s finalizer", raftMemberKey)
		removeFinalizer(member, raftMemberKey)
		if err := r.client.Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return err
		}
		return nil
	}

	partitionName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      fmt.Sprintf("%s-%s", storeName.Name, member.Annotations[raftPartitionKey]),
	}
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, partitionName, partition); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debugf("Removing %s finalizer", raftMemberKey)
		removeFinalizer(member, raftMemberKey)
		if err := r.client.Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return err
		}
		return nil
	}

	podName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Spec.Pod.Name,
	}
	pod := &corev1.Pod{}
	if err := r.client.Get(ctx, podName, pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debugf("Removing %s finalizer", raftMemberKey)
		removeFinalizer(member, raftMemberKey)
		if err := r.client.Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return err
		}
		return nil
	}

	if ok, err := r.removeMember(ctx, log, store, cluster, partition, pod, member); err != nil {
		return err
	} else if ok {
		log.Debugf("Removing %s finalizer", raftMemberKey)
		removeFinalizer(member, raftMemberKey)
		if err := r.client.Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return err
		}
		return nil
	}
	return nil
}

func (r *RaftMemberReconciler) addMember(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, pod *corev1.Pod, member *raftv1beta2.RaftMember) (bool, error) {
	if member.Status.PodRef == nil || member.Status.PodRef.UID != pod.UID {
		log.Info("Pod UID has changed; reverting member status")
		member.Status.PodRef = &corev1.ObjectReference{
			APIVersion: pod.APIVersion,
			Kind:       pod.Kind,
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        pod.UID,
		}
		member.Status.Version = nil
		if err := r.client.Status().Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return false, err
		}
		return true, nil
	}

	var containerVersion int32
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == nodeContainerName {
			containerVersion = containerStatus.RestartCount + 1
			break
		}
	}

	if member.Status.Version == nil || containerVersion > *member.Status.Version {
		if member.Status.State != raftv1beta2.RaftMemberNotReady {
			member.Status.State = raftv1beta2.RaftMemberNotReady
			if err := r.client.Status().Update(ctx, member); err != nil {
				if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
					log.Error(err)
				}
				return false, err
			}
			r.events.Eventf(member, "Normal", "StateChanged", "State changed to %s", member.Status.State)
			return true, nil
		}

		switch member.Spec.BootstrapPolicy {
		case raftv1beta2.RaftBootstrap:
			replicas := make([]raftv1.ReplicaConfig, 0, len(member.Spec.Config.Peers))
			for _, peer := range member.Spec.Config.Peers {
				host := getClusterPodDNSName(cluster, peer.Pod.Name)
				replicas = append(replicas, raftv1.ReplicaConfig{
					ReplicaID: raftv1.ReplicaID(peer.ReplicaID),
					Host:      host,
					Port:      protocolPort,
					Role:      raftv1.ReplicaRole_MEMBER,
				})
			}

			address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
			conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Error(err)
				return false, err
			}
			defer conn.Close()

			// Bootstrap the member with the initial configuration
			log.Infof("Boostrapping new shard")
			client := raftv1.NewNodeClient(conn)
			request := &raftv1.BootstrapShardRequest{
				ShardID:   raftv1.ShardID(member.Spec.ShardID),
				ReplicaID: raftv1.ReplicaID(member.Spec.MemberID),
				Replicas:  replicas,
			}
			if _, err := client.BootstrapShard(ctx, request); err != nil {
				log.Error(err)
				err = errors.FromProto(err)
				if !errors.IsAlreadyExists(err) {
					r.events.Eventf(store, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d member %d: %s", partition.Spec.PartitionID, member.Spec.ReplicaID, err.Error())
					r.events.Eventf(cluster, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d member %d: %s", partition.Spec.PartitionID, member.Spec.ReplicaID, err.Error())
					r.events.Eventf(partition, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d member %d: %s", partition.Spec.PartitionID, member.Spec.ReplicaID, err.Error())
					r.events.Eventf(pod, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d member %d: %s", partition.Spec.PartitionID, member.Spec.ReplicaID, err.Error())
					r.events.Eventf(member, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d: %s", partition.Spec.PartitionID, err.Error())
					return false, err
				}
			} else {
				r.events.Eventf(store, "Normal", "Bootstrapped", "Bootstrapped partition %d member %d", partition.Spec.PartitionID, member.Spec.ReplicaID)
				r.events.Eventf(cluster, "Normal", "Bootstrapped", "Bootstrapped partition %d member %d", partition.Spec.PartitionID, member.Spec.ReplicaID)
				r.events.Eventf(partition, "Normal", "Bootstrapped", "Bootstrapped partition %d member %d", partition.Spec.PartitionID, member.Spec.ReplicaID)
				r.events.Eventf(pod, "Normal", "Bootstrapped", "Bootstrapped partition %d member %d", partition.Spec.PartitionID, member.Spec.ReplicaID)
				r.events.Eventf(member, "Normal", "Bootstrapped", "Bootstrapped partition %d", partition.Spec.PartitionID)
			}

			member.Status.Version = &containerVersion
			if err := r.client.Status().Update(ctx, member); err != nil {
				if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
					log.Error(err)
				}
				return false, err
			}
			return true, nil
		case raftv1beta2.RaftJoin:
			// Loop through the list of peers and attempt to add the member to the Raft group until successful
			var returnErr error
			for _, peer := range member.Spec.Config.Peers {
				if ok, err := r.tryAddMember(ctx, log, store, cluster, partition, member, peer); err != nil {
					returnErr = err
				} else if ok {
					address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
					conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						log.Error(err)
						return false, err
					}

					// Bootstrap the member by joining it to the cluster
					log.Infof("Joining existing shard")
					client := raftv1.NewNodeClient(conn)
					request := &raftv1.JoinShardRequest{
						ShardID:   raftv1.ShardID(member.Spec.ShardID),
						ReplicaID: raftv1.ReplicaID(member.Spec.MemberID),
					}
					if _, err := client.JoinShard(ctx, request); err != nil {
						log.Error(err)
						err = errors.FromProto(err)
						if !errors.IsAlreadyExists(err) {
							r.events.Eventf(store, "Warning", "JoinFailed", "Failed to join member %d to partition %d: %s", member.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(cluster, "Warning", "JoinFailed", "Failed to join member %d to partition %d: %s", member.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(partition, "Warning", "JoinFailed", "Failed to join member %d to partition %d: %s", member.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(pod, "Warning", "JoinFailed", "Failed to join member %d to partition %d: %s", member.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(member, "Warning", "JoinFailed", "Failed to join partition %d: %s", partition.Spec.PartitionID, err.Error())
							_ = conn.Close()
							return false, err
						}
					} else {
						r.events.Eventf(store, "Normal", "Joined", "Joined member %d to partition %d", member.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(cluster, "Normal", "Joined", "Joined member %d to partition %d", member.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(partition, "Normal", "Joined", "Joined member %d to partition %d", member.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(pod, "Normal", "Joined", "Joined member %d to partition %d", member.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(member, "Normal", "Joined", "Joined partition %d", partition.Spec.PartitionID)
					}
					_ = conn.Close()

					member.Status.Version = &containerVersion
					if err := r.client.Status().Update(ctx, member); err != nil {
						if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
							log.Error(err)
						}
						return false, err
					}
					return true, nil
				}
			}
			return false, returnErr
		}
	}
	return false, nil
}

func (r *RaftMemberReconciler) tryAddMember(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, member *raftv1beta2.RaftMember, peer raftv1beta2.RaftMemberReference) (bool, error) {
	pod := &corev1.Pod{}
	podName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      peer.Pod.Name,
	}
	if err := r.client.Get(ctx, podName, pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Warn(err)
		}
		return false, err
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer conn.Close()

	client := raftv1.NewNodeClient(conn)
	getConfigRequest := &raftv1.GetConfigRequest{
		ShardID: raftv1.ShardID(member.Spec.ShardID),
	}
	getConfigResponse, err := client.GetConfig(ctx, getConfigRequest)
	if err != nil {
		log.Warn(err)
		return false, err
	}

	log.Info("Adding replica to shard")
	addReplicaRequest := &raftv1.AddReplicaRequest{
		ShardID: raftv1.ShardID(member.Spec.ShardID),
		Replica: raftv1.ReplicaConfig{
			ReplicaID: raftv1.ReplicaID(member.Spec.MemberID),
			Host:      getClusterPodDNSName(cluster, member.Spec.Pod.Name),
			Port:      protocolPort,
		},
		Version: getConfigResponse.Shard.Version,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if _, err := client.AddReplica(ctx, addReplicaRequest); err != nil {
		log.Warn(err)
		r.events.Eventf(store, "Warning", "ConfigurationChangeFailed", "Failed to add member %d to partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(cluster, "Warning", "ConfigurationChangeFailed", "Failed to add member %d to partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(partition, "Warning", "ConfigurationChangeFailed", "Failed to add member %d to partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(pod, "Warning", "ConfigurationChangeFailed", "Failed to add member %d to partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(member, "Warning", "ConfigurationChangeFailed", "Failed to add member to partition %d: %s", partition.Spec.PartitionID, err.Error())
		return false, err
	}
	r.events.Eventf(store, "Normal", "ConfigurationChanged", "Added member %d to partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(cluster, "Normal", "ConfigurationChanged", "Added member %d to partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(partition, "Normal", "ConfigurationChanged", "Added member %d to partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(pod, "Normal", "ConfigurationChanged", "Added member %d to partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(member, "Normal", "ConfigurationChanged", "Added member to partition %d", partition.Spec.PartitionID)
	return true, nil
}

func (r *RaftMemberReconciler) removeMember(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, pod *corev1.Pod, member *raftv1beta2.RaftMember) (bool, error) {
	if member.Status.State != raftv1beta2.RaftMemberNotReady {
		member.Status.State = raftv1beta2.RaftMemberNotReady
		if err := r.client.Status().Update(ctx, member); err != nil {
			if !k8serrors.IsNotFound(err) && !k8serrors.IsConflict(err) {
				log.Error(err)
			}
			return false, err
		}
		r.events.Eventf(member, "Normal", "StateChanged", "State changed to %s", member.Status.State)
		return true, nil
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error(err)
		return false, err
	}
	defer conn.Close()

	// Shutdown the group member.
	log.Infof("Stopping replica")
	client := raftv1.NewNodeClient(conn)
	request := &raftv1.LeaveShardRequest{
		ShardID: raftv1.ShardID(member.Spec.ShardID),
	}
	if _, err := client.LeaveShard(ctx, request); err != nil {
		log.Warn(err)
	}

	// Loop through the list of peers and attempt to remove the member from the Raft group until successful
	var returnErr error
	for _, peer := range member.Spec.Config.Peers {
		if ok, err := r.tryRemoveMember(ctx, log, store, cluster, partition, member, peer); err != nil {
			returnErr = err
		} else if ok {
			return true, nil
		}
	}
	return false, returnErr
}

func (r *RaftMemberReconciler) tryRemoveMember(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, member *raftv1beta2.RaftMember, peer raftv1beta2.RaftMemberReference) (bool, error) {
	pod := &corev1.Pod{}
	podName := types.NamespacedName{
		Namespace: member.Namespace,
		Name:      peer.Pod.Name,
	}
	if err := r.client.Get(ctx, podName, pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Warn(err)
		}
		return false, err
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	conn, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Warn(err)
		return false, err
	}
	defer conn.Close()

	client := raftv1.NewNodeClient(conn)
	getConfigRequest := &raftv1.GetConfigRequest{
		ShardID: raftv1.ShardID(member.Spec.ShardID),
	}
	getConfigResponse, err := client.GetConfig(ctx, getConfigRequest)
	if err != nil {
		log.Warn(err)
		return false, err
	}

	log.Infof("Removing replica from shard")
	removeReplicaRequest := &raftv1.RemoveReplicaRequest{
		ShardID:   raftv1.ShardID(member.Spec.ShardID),
		ReplicaID: raftv1.ReplicaID(member.Spec.MemberID),
		Version:   getConfigResponse.Shard.Version,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if _, err := client.RemoveReplica(ctx, removeReplicaRequest); err != nil {
		log.Warn(err)
		r.events.Eventf(store, "Warning", "ConfigurationChangeFailed", "Failed to remove member %d from partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(cluster, "Warning", "ConfigurationChangeFailed", "Failed to remove member %d from partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(partition, "Warning", "ConfigurationChangeFailed", "Failed to remove member %d from partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(pod, "Warning", "ConfigurationChangeFailed", "Failed to remove member %d from partition %d: %s", member.Spec.MemberID, partition.Spec.PartitionID, err.Error())
		r.events.Eventf(member, "Warning", "ConfigurationChangeFailed", "Failed to remove member from partition %d: %s", partition.Spec.PartitionID, err.Error())
		return false, err
	}
	r.events.Eventf(store, "Normal", "ConfigurationChanged", "Removed member %d from partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(cluster, "Normal", "ConfigurationChanged", "Removed member %d from partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(partition, "Normal", "ConfigurationChanged", "Removed member %d from partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(pod, "Normal", "ConfigurationChanged", "Removed member %d from partition %d", member.Spec.MemberID, partition.Spec.PartitionID)
	r.events.Eventf(member, "Normal", "ConfigurationChanged", "Removed member from partition %d", partition.Spec.PartitionID)
	return true, nil
}

var _ reconcile.Reconciler = (*RaftMemberReconciler)(nil)
