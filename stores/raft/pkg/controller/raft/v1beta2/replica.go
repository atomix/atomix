// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	raftv1 "github.com/atomix/atomix/stores/raft/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
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

func addRaftReplicaController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &RaftReplicaReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-raft"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-raft-replica", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftReplica{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Pod
	err = controller.Watch(&source.Kind{Type: &corev1.Pod{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		clusterName, ok := object.GetAnnotations()[raftClusterKey]
		if !ok {
			return nil
		}

		options := &client.ListOptions{
			LabelSelector: labels.SelectorFromSet(map[string]string{
				raftNamespaceKey: object.GetAnnotations()[raftNamespaceKey],
				raftClusterKey:   clusterName,
			}),
		}
		replicas := &raftv1beta2.RaftReplicaList{}
		if err := mgr.GetClient().List(context.Background(), replicas, options); err != nil {
			return nil
		}

		var requests []reconcile.Request
		for _, replica := range replicas.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: replica.Namespace,
					Name:      replica.Name,
				},
			})
		}
		return requests
	}))
	return nil
}

// RaftReplicaReconciler reconciles a RaftReplica object
type RaftReplicaReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
	mu     sync.RWMutex
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *RaftReplicaReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("RaftReplica", request.NamespacedName.String()))
	log.Debug("Reconciling RaftReplica")

	replica := &raftv1beta2.RaftReplica{}
	if ok, err := get(r.client, ctx, request.NamespacedName, replica, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}

	if replica.DeletionTimestamp != nil {
		if err := r.reconcileDelete(ctx, log, replica); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if err := r.reconcileCreate(ctx, log, replica); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *RaftReplicaReconciler) reconcileCreate(ctx context.Context, log logging.Logger, replica *raftv1beta2.RaftReplica) error {
	if !hasFinalizer(replica, raftReplicaIDKey) {
		log.Debugf("Adding %s finalizer", raftReplicaIDKey)
		addFinalizer(replica, raftReplicaIDKey)
		return update(r.client, ctx, replica, log)
	}

	storeName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      replica.Annotations[raftStoreKey],
	}
	store := &raftv1beta2.RaftStore{}
	if ok, err := get(r.client, ctx, storeName, store, log); err != nil {
		return err
	} else if !ok {
		return nil
	}

	clusterName := types.NamespacedName{
		Namespace: replica.Annotations[raftNamespaceKey],
		Name:      replica.Annotations[raftClusterKey],
	}
	cluster := &raftv1beta2.RaftCluster{}
	if ok, err := get(r.client, ctx, clusterName, cluster, log); err != nil {
		return err
	} else if !ok {
		return nil
	}

	partitionName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      fmt.Sprintf("%s-%s", storeName.Name, replica.Annotations[raftPartitionIDKey]),
	}
	partition := &raftv1beta2.RaftPartition{}
	if ok, err := get(r.client, ctx, partitionName, partition, log); err != nil {
		return err
	} else if !ok {
		return nil
	}

	podName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      replica.Spec.Pod.Name,
	}
	pod := &corev1.Pod{}
	if ok, err := get(r.client, ctx, podName, pod, log); err != nil {
		return err
	} else if !ok {
		return nil
	}

	log = log.WithFields(
		logging.Uint64("PartitionID", uint64(partition.Spec.PartitionID)),
		logging.Uint64("GroupID", uint64(partition.Spec.GroupID)),
		logging.Uint64("ReplicaID", uint64(replica.Spec.ReplicaID)),
		logging.Uint64("MemberID", uint64(replica.Spec.MemberID)),
		logging.Stringer("Pod", podName))
	if ok, err := r.addReplica(ctx, log, store, cluster, partition, pod, replica); err != nil {
		return err
	} else if ok {
		return nil
	}
	return nil
}

func (r *RaftReplicaReconciler) reconcileDelete(ctx context.Context, log logging.Logger, replica *raftv1beta2.RaftReplica) error {
	if !hasFinalizer(replica, raftReplicaIDKey) {
		return nil
	}

	storeName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      replica.Annotations[raftStoreKey],
	}
	store := &raftv1beta2.RaftStore{}
	if err := r.client.Get(ctx, storeName, store); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debug(err)
		log.Debugf("Removing %s finalizer", raftReplicaIDKey)
		removeFinalizer(replica, raftReplicaIDKey)
		return update(r.client, ctx, replica, log)
	}

	clusterName := types.NamespacedName{
		Namespace: replica.Annotations[raftNamespaceKey],
		Name:      replica.Annotations[raftClusterKey],
	}
	cluster := &raftv1beta2.RaftCluster{}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debug(err)
		log.Debugf("Removing %s finalizer", raftReplicaIDKey)
		removeFinalizer(replica, raftReplicaIDKey)
		return update(r.client, ctx, replica, log)
	}

	partitionName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      fmt.Sprintf("%s-%s", storeName.Name, replica.Annotations[raftPartitionIDKey]),
	}
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, partitionName, partition); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debug(err)
		log.Debugf("Removing %s finalizer", raftReplicaIDKey)
		removeFinalizer(replica, raftReplicaIDKey)
		return update(r.client, ctx, replica, log)
	}

	podName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      replica.Spec.Pod.Name,
	}
	pod := &corev1.Pod{}
	if err := r.client.Get(ctx, podName, pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return err
		}
		log.Debug(err)
		log.Debugf("Removing %s finalizer", raftReplicaIDKey)
		removeFinalizer(replica, raftReplicaIDKey)
		return update(r.client, ctx, replica, log)
	}

	if ok, err := r.removeReplica(ctx, log, store, cluster, partition, pod, replica); err != nil {
		return err
	} else if ok {
		log.Debugf("Removing %s finalizer", raftReplicaIDKey)
		removeFinalizer(replica, raftReplicaIDKey)
		return update(r.client, ctx, replica, log)
	}
	return nil
}

func (r *RaftReplicaReconciler) addReplica(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, pod *corev1.Pod, replica *raftv1beta2.RaftReplica) (bool, error) {
	if replica.Status.PodRef == nil || replica.Status.PodRef.UID != pod.UID {
		log.Info("Pod UID has changed; reverting replica status")
		replica.Status.PodRef = &corev1.ObjectReference{
			APIVersion: pod.APIVersion,
			Kind:       pod.Kind,
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        pod.UID,
		}
		replica.Status.Version = nil
		return true, updateStatus(r.client, ctx, replica, log)
	}

	var containerVersion int32
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == nodeContainerName {
			containerVersion = containerStatus.RestartCount + 1
			break
		}
	}

	if replica.Status.Version == nil || containerVersion > *replica.Status.Version {
		if replica.Status.State != raftv1beta2.RaftReplicaNotReady {
			replica.Status.State = raftv1beta2.RaftReplicaNotReady
			if err := updateStatus(r.client, ctx, replica, log); err != nil {
				return false, err
			}
			log.Infow("RaftReplica status changed", logging.String("Status", string(replica.Status.State)))
			r.events.Eventf(replica, "Normal", "StateChanged", "State changed to %s", replica.Status.State)
			return true, nil
		}

		var config raftv1.RaftConfig
		if replica.Spec.Config.ElectionRTT != nil {
			config.ElectionRTT = *replica.Spec.Config.ElectionRTT
		}
		if replica.Spec.Config.HeartbeatRTT != nil {
			config.ElectionRTT = *replica.Spec.Config.HeartbeatRTT
		}
		if replica.Spec.Config.SnapshotEntries != nil {
			config.SnapshotEntries = uint64(*replica.Spec.Config.SnapshotEntries)
		}
		if replica.Spec.Config.CompactionOverhead != nil {
			config.CompactionOverhead = uint64(*replica.Spec.Config.CompactionOverhead)
		}
		if replica.Spec.Config.MaxInMemLogSize != nil {
			if maxInMemLogSize, ok := replica.Spec.Config.MaxInMemLogSize.AsInt64(); ok {
				config.MaxInMemLogSize = uint64(maxInMemLogSize)
			}
		}

		if !replica.Spec.Join {
			if pod.Status.PodIP == "" {
				return false, nil
			}

			members := make([]raftv1.MemberConfig, 0, int(replica.Spec.Peers))
			for ordinal := 1; ordinal <= int(replica.Spec.Peers); ordinal++ {
				peerID := raftv1beta2.ReplicaID(ordinal)
				members = append(members, raftv1.MemberConfig{
					MemberID: raftv1.MemberID(peerID),
					Host:     getClusterPodDNSName(cluster, getReplicaPodName(cluster, partition, peerID)),
					Port:     raftPort,
					Role:     raftv1.MemberRole_MEMBER,
				})
			}

			address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
			conn, err := grpc.DialContext(ctx, address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
				grpc.WithStreamInterceptor(interceptors.ErrorHandlingStreamClientInterceptor()))
			if err != nil {
				if !errors.IsUnavailable(err) {
					log.Warn(err)
				}
				return false, err
			}
			defer conn.Close()

			// Bootstrap the replica with the initial configuration
			log.Infof("Bootstrapping replica")
			node := raftv1.NewNodeClient(conn)
			request := &raftv1.BootstrapGroupRequest{
				GroupID:  raftv1.GroupID(replica.Spec.GroupID),
				MemberID: raftv1.MemberID(replica.Spec.MemberID),
				Members:  members,
				Config:   config,
			}
			if _, err := node.BootstrapGroup(ctx, request); err != nil {
				if !errors.IsUnavailable(err) && !errors.IsAlreadyExists(err) {
					log.Warn(err)
					r.events.Eventf(store, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d replica %d: %s", partition.Spec.PartitionID, replica.Spec.ReplicaID, err.Error())
					r.events.Eventf(cluster, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d replica %d: %s", partition.Spec.PartitionID, replica.Spec.ReplicaID, err.Error())
					r.events.Eventf(partition, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d replica %d: %s", partition.Spec.PartitionID, replica.Spec.ReplicaID, err.Error())
					r.events.Eventf(pod, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d replica %d: %s", partition.Spec.PartitionID, replica.Spec.ReplicaID, err.Error())
					r.events.Eventf(replica, "Warning", "BootstrapFailed", "Failed to bootstrap partition %d: %s", partition.Spec.PartitionID, err.Error())
				}
				return false, err
			} else {
				r.events.Eventf(store, "Normal", "Bootstrapped", "Bootstrapped partition %d replica %d", partition.Spec.PartitionID, replica.Spec.ReplicaID)
				r.events.Eventf(cluster, "Normal", "Bootstrapped", "Bootstrapped partition %d replica %d", partition.Spec.PartitionID, replica.Spec.ReplicaID)
				r.events.Eventf(partition, "Normal", "Bootstrapped", "Bootstrapped partition %d replica %d", partition.Spec.PartitionID, replica.Spec.ReplicaID)
				r.events.Eventf(pod, "Normal", "Bootstrapped", "Bootstrapped partition %d replica %d", partition.Spec.PartitionID, replica.Spec.ReplicaID)
				r.events.Eventf(replica, "Normal", "Bootstrapped", "Bootstrapped partition %d", partition.Spec.PartitionID)
			}

			replica.Status.Version = &containerVersion
			if err := updateStatus(r.client, ctx, replica, log); err != nil {
				return false, err
			}
			return true, nil
		} else {
			// Loop through the replicas in the partition and attempt to add the replica to the Raft group until successful
			options := &client.ListOptions{
				Namespace:     replica.Namespace,
				LabelSelector: labels.SelectorFromSet(newPartitionSelector(partition)),
			}
			peers := &raftv1beta2.RaftReplicaList{}
			if err := r.client.List(ctx, peers, options); err != nil {
				return false, err
			}

			var returnErr error
			for _, peer := range peers.Items {
				if ok, err := r.tryAddReplica(ctx, log, store, cluster, partition, replica, &peer); err != nil {
					returnErr = err
				} else if ok {
					if pod.Status.PodIP == "" {
						continue
					}

					address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
					conn, err := grpc.DialContext(ctx, address,
						grpc.WithTransportCredentials(insecure.NewCredentials()),
						grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
						grpc.WithStreamInterceptor(interceptors.ErrorHandlingStreamClientInterceptor()))
					if err != nil {
						if !errors.IsUnavailable(err) {
							log.Warn(err)
						}
						return false, err
					}

					// Bootstrap the replica by joining it to the cluster
					log.Infof("Joining existing group")
					client := raftv1.NewNodeClient(conn)
					request := &raftv1.JoinGroupRequest{
						GroupID:  raftv1.GroupID(replica.Spec.GroupID),
						MemberID: raftv1.MemberID(replica.Spec.MemberID),
						Config:   config,
					}
					_, err = client.JoinGroup(ctx, request)
					_ = conn.Close()
					if err != nil {
						if !errors.IsUnavailable(err) && !errors.IsAlreadyExists(err) {
							log.Warn(err)
							r.events.Eventf(store, "Warning", "JoinFailed", "Failed to join replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(cluster, "Warning", "JoinFailed", "Failed to join replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(partition, "Warning", "JoinFailed", "Failed to join replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(pod, "Warning", "JoinFailed", "Failed to join replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
							r.events.Eventf(replica, "Warning", "JoinFailed", "Failed to join partition %d: %s", partition.Spec.PartitionID, err.Error())
						}
						return false, err
					} else {
						r.events.Eventf(store, "Normal", "Joined", "Joined replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(cluster, "Normal", "Joined", "Joined replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(partition, "Normal", "Joined", "Joined replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(pod, "Normal", "Joined", "Joined replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
						r.events.Eventf(replica, "Normal", "Joined", "Joined partition %d", partition.Spec.PartitionID)
					}

					replica.Status.Version = &containerVersion
					if err := updateStatus(r.client, ctx, replica, log); err != nil {
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

func (r *RaftReplicaReconciler) tryAddReplica(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, replica *raftv1beta2.RaftReplica, peer *raftv1beta2.RaftReplica) (bool, error) {
	pod := &corev1.Pod{}
	podName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      peer.Spec.Pod.Name,
	}
	if ok, err := get(r.client, ctx, podName, pod, log); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}

	if pod.Status.PodIP == "" {
		return false, nil
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptors.ErrorHandlingStreamClientInterceptor()))
	if err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
		}
		return false, err
	}
	defer conn.Close()

	client := raftv1.NewNodeClient(conn)
	getConfigRequest := &raftv1.GetConfigRequest{
		GroupID: raftv1.GroupID(replica.Spec.GroupID),
	}
	getConfigResponse, err := client.GetConfig(ctx, getConfigRequest)
	if err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
		}
		return false, err
	}

	log.Info("Adding member to group")
	addMemberRequest := &raftv1.AddMemberRequest{
		GroupID: raftv1.GroupID(replica.Spec.GroupID),
		Member: raftv1.MemberConfig{
			MemberID: raftv1.MemberID(replica.Spec.MemberID),
			Host:     getClusterPodDNSName(cluster, replica.Spec.Pod.Name),
			Port:     raftPort,
		},
		Version: getConfigResponse.Group.Version,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if _, err := client.AddMember(ctx, addMemberRequest); err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
			r.events.Eventf(store, "Warning", "ConfigurationChangeFailed", "Failed to add replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(cluster, "Warning", "ConfigurationChangeFailed", "Failed to add replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(partition, "Warning", "ConfigurationChangeFailed", "Failed to add replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(pod, "Warning", "ConfigurationChangeFailed", "Failed to add replica %d to partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(replica, "Warning", "ConfigurationChangeFailed", "Failed to add replica to partition %d: %s", partition.Spec.PartitionID, err.Error())
		}
		return false, err
	}
	r.events.Eventf(store, "Normal", "ConfigurationChanged", "Added replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(cluster, "Normal", "ConfigurationChanged", "Added replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(partition, "Normal", "ConfigurationChanged", "Added replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(pod, "Normal", "ConfigurationChanged", "Added replica %d to partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(replica, "Normal", "ConfigurationChanged", "Added replica to partition %d", partition.Spec.PartitionID)
	return true, nil
}

func (r *RaftReplicaReconciler) removeReplica(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, pod *corev1.Pod, replica *raftv1beta2.RaftReplica) (bool, error) {
	if replica.Status.State != raftv1beta2.RaftReplicaNotReady {
		replica.Status.State = raftv1beta2.RaftReplicaNotReady
		if err := updateStatus(r.client, ctx, replica, log); err != nil {
			return false, err
		}
		log.Infow("RaftReplica status changed", logging.String("Status", string(replica.Status.State)))
		r.events.Eventf(replica, "Normal", "StateChanged", "State changed to %s", replica.Status.State)
		return true, nil
	}

	if pod.Status.PodIP == "" {
		return false, nil
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptors.ErrorHandlingStreamClientInterceptor()))
	if err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
		}
		return false, err
	}
	defer conn.Close()

	// Shutdown the group replica.
	log.Infof("Stopping member")
	node := raftv1.NewNodeClient(conn)
	request := &raftv1.LeaveGroupRequest{
		GroupID: raftv1.GroupID(replica.Spec.GroupID),
	}
	if _, err := node.LeaveGroup(ctx, request); err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
		}
	}

	// Loop through the list of peers and attempt to remove the replica from the Raft group until successful
	options := &client.ListOptions{
		Namespace:     replica.Namespace,
		LabelSelector: labels.SelectorFromSet(newPartitionSelector(partition)),
	}
	peers := &raftv1beta2.RaftReplicaList{}
	if err := r.client.List(ctx, peers, options); err != nil {
		return false, err
	}

	var returnErr error
	for _, peer := range peers.Items {
		if ok, err := r.tryRemoveReplica(ctx, log, store, cluster, partition, replica, &peer); err != nil {
			returnErr = err
		} else if ok {
			return true, nil
		}
	}
	return false, returnErr
}

func (r *RaftReplicaReconciler) tryRemoveReplica(ctx context.Context, log logging.Logger, store *raftv1beta2.RaftStore, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, replica *raftv1beta2.RaftReplica, peer *raftv1beta2.RaftReplica) (bool, error) {
	pod := &corev1.Pod{}
	podName := types.NamespacedName{
		Namespace: replica.Namespace,
		Name:      peer.Spec.Pod.Name,
	}
	if ok, err := get(r.client, ctx, podName, pod, log); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}

	if pod.Status.PodIP == "" {
		return false, nil
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptors.ErrorHandlingStreamClientInterceptor()))
	if err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
		}
		return false, err
	}
	defer conn.Close()

	client := raftv1.NewNodeClient(conn)
	getConfigRequest := &raftv1.GetConfigRequest{
		GroupID: raftv1.GroupID(replica.Spec.GroupID),
	}
	getConfigResponse, err := client.GetConfig(ctx, getConfigRequest)
	if err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
		}
		return false, err
	}

	log.Infof("Removing member from group")
	removeMemberRequest := &raftv1.RemoveMemberRequest{
		GroupID:  raftv1.GroupID(replica.Spec.GroupID),
		MemberID: raftv1.MemberID(replica.Spec.MemberID),
		Version:  getConfigResponse.Group.Version,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if _, err := client.RemoveMember(ctx, removeMemberRequest); err != nil {
		if !errors.IsUnavailable(err) {
			log.Warn(err)
			r.events.Eventf(store, "Warning", "ConfigurationChangeFailed", "Failed to remove replica %d from partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(cluster, "Warning", "ConfigurationChangeFailed", "Failed to remove replica %d from partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(partition, "Warning", "ConfigurationChangeFailed", "Failed to remove replica %d from partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(pod, "Warning", "ConfigurationChangeFailed", "Failed to remove replica %d from partition %d: %s", replica.Spec.ReplicaID, partition.Spec.PartitionID, err.Error())
			r.events.Eventf(replica, "Warning", "ConfigurationChangeFailed", "Failed to remove replica from partition %d: %s", partition.Spec.PartitionID, err.Error())
		}
		return false, err
	}
	r.events.Eventf(store, "Normal", "ConfigurationChanged", "Removed replica %d from partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(cluster, "Normal", "ConfigurationChanged", "Removed replica %d from partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(partition, "Normal", "ConfigurationChanged", "Removed replica %d from partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(pod, "Normal", "ConfigurationChanged", "Removed replica %d from partition %d", replica.Spec.ReplicaID, partition.Spec.PartitionID)
	r.events.Eventf(replica, "Normal", "ConfigurationChanged", "Removed replica from partition %d", partition.Spec.PartitionID)
	return true, nil
}

var _ reconcile.Reconciler = (*RaftReplicaReconciler)(nil)
