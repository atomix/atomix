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
	"github.com/cenkalti/backoff"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
	"sync"
	"time"

	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func addPodController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &PodReconciler{
			client:   mgr.GetClient(),
			scheme:   mgr.GetScheme(),
			events:   mgr.GetEventRecorderFor("atomix-raft-storage"),
			watchers: make(map[string]context.CancelFunc),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-raft-pod", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to raft Pods
	err = controller.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}
	return nil
}

// PodReconciler reconciles a Pod object
type PodReconciler struct {
	client   client.Client
	scheme   *runtime.Scheme
	events   record.EventRecorder
	watchers map[string]context.CancelFunc
	mu       sync.RWMutex
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *PodReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("Pod", request.NamespacedName.String()))

	pod := &corev1.Pod{}
	if ok, err := get(r.client, ctx, request.NamespacedName, pod, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}

	if _, ok := pod.Annotations[raftClusterKey]; !ok {
		return reconcile.Result{}, nil
	}

	log.Debug("Reconciling Pod")

	if pod.DeletionTimestamp != nil {
		if err := r.reconcileDelete(ctx, log, pod); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if err := r.reconcileCreate(ctx, log, pod); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *PodReconciler) reconcileCreate(ctx context.Context, log logging.Logger, pod *corev1.Pod) error {
	if !hasFinalizer(pod, podKey) {
		log.Infof("Adding %s finalizer", podKey)
		addFinalizer(pod, podKey)
		return update(r.client, ctx, pod, log)
	}

	clusterName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Annotations[raftClusterKey],
	}
	address := fmt.Sprintf("%s:%d", getDNSName(clusterName.Namespace, clusterName.Name, pod.Name), apiPort)
	if err := r.watch(log, clusterName, address); err != nil {
		return err
	}
	return nil
}

func (r *PodReconciler) reconcileDelete(ctx context.Context, log logging.Logger, pod *corev1.Pod) error {
	if !hasFinalizer(pod, podKey) {
		return nil
	}

	address := fmt.Sprintf("%s:%d", getDNSName(pod.Namespace, pod.Annotations[raftClusterKey], pod.Name), apiPort)
	if err := r.unwatch(address); err != nil {
		return err
	}

	log.Infof("Removing %s finalizer", podKey)
	removeFinalizer(pod, podKey)
	return update(r.client, ctx, pod, log)
}

func (r *PodReconciler) watch(log logging.Logger, clusterName types.NamespacedName, address string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.watchers[address]; ok {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	r.watchers[address] = cancel

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.watchers, address)
			r.mu.Unlock()
		}()

		log.Info("Creating new Watch")
		conn, err := grpc.Dial(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(
				interceptors.ErrorHandlingUnaryClientInterceptor(),
				interceptors.RetryingUnaryClientInterceptor()),
			grpc.WithChainStreamInterceptor(
				interceptors.ErrorHandlingStreamClientInterceptor(),
				interceptors.RetryingStreamClientInterceptor()))
		if err != nil {
			log.Warn(err)
			return
		}

		node := raftv1.NewNodeClient(conn)

		request := &raftv1.WatchRequest{}
		stream, err := node.Watch(ctx, request)
		if err != nil {
			log.Warn(err)
			return
		}

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				log.Debug("Watch complete")
				return
			}
			if err != nil {
				if errors.IsCanceled(err) {
					log.Warn("Watch canceled")
					return
				}
				log.Warn(err)
			} else {
				log.Debugw("Received event", logging.Stringer("Event", event))
				timestamp := metav1.NewTime(event.Timestamp)
				switch e := event.Event.(type) {
				case *raftv1.Event_MemberReady:
					r.recordReplicaEvent(ctx, log, clusterName, e.MemberReady.GroupID, e.MemberReady.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							if status.State != raftv1beta2.RaftReplicaReady {
								status.State = raftv1beta2.RaftReplicaReady
								status.LastUpdated = &timestamp
								return true
							}
							return false
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "StateChanged", "Replica is ready")
						})
					r.recordPartitionEvent(ctx, log, clusterName, e.MemberReady.GroupID,
						func(status *raftv1beta2.RaftPartitionStatus) bool {
							replica, err := r.getReplica(ctx, log, clusterName, raftv1beta2.GroupID(e.MemberReady.GroupID), raftv1beta2.MemberID(e.MemberReady.MemberID))
							if err != nil {
								return false
							}
							if status.Leader != nil && status.Leader.Name == replica.Name {
								return false
							}
							for _, follower := range status.Followers {
								if follower.Name == replica.Name {
									return false
								}
							}
							status.Followers = append(status.Followers, corev1.LocalObjectReference{
								Name: replica.Name,
							})
							return true
						}, func(group *raftv1beta2.RaftPartition) {})
				case *raftv1.Event_LeaderUpdated:
					r.recordReplicaEvent(ctx, log, clusterName, e.LeaderUpdated.GroupID, e.LeaderUpdated.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							term := uint64(e.LeaderUpdated.Term)
							if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
								role := raftv1beta2.RaftFollower
								if e.LeaderUpdated.Leader == 0 {
									status.Leader = nil
								} else {
									leader, err := r.getReplica(ctx, log, clusterName, raftv1beta2.GroupID(e.LeaderUpdated.GroupID), raftv1beta2.MemberID(e.LeaderUpdated.Leader))
									if err != nil {
										return false
									}
									status.Leader = &corev1.LocalObjectReference{
										Name: leader.Name,
									}
									if e.LeaderUpdated.Leader == e.LeaderUpdated.MemberID {
										role = raftv1beta2.RaftLeader
									}
								}
								status.Term = &term
								status.Role = &role
								status.LastUpdated = &timestamp
								return true
							}
							return false
						}, func(replica *raftv1beta2.RaftReplica) {
							if replica.Status.Role != nil && *replica.Status.Role == raftv1beta2.RaftLeader {
								r.events.Eventf(replica, "Normal", "ElectedLeader", "Elected leader for term %d", e.LeaderUpdated.Term)
							}
						})
					r.recordPartitionEvent(ctx, log, clusterName, e.LeaderUpdated.GroupID,
						func(status *raftv1beta2.RaftPartitionStatus) bool {
							term := uint64(e.LeaderUpdated.Term)
							if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
								var newLeader *corev1.LocalObjectReference
								if e.LeaderUpdated.Leader != 0 {
									leader, err := r.getReplica(ctx, log, clusterName, raftv1beta2.GroupID(e.LeaderUpdated.GroupID), raftv1beta2.MemberID(e.LeaderUpdated.Leader))
									if err != nil {
										return false
									}
									newLeader = &corev1.LocalObjectReference{
										Name: leader.Name,
									}
								}
								var newFollowers []corev1.LocalObjectReference
								for _, follower := range status.Followers {
									if newLeader == nil || follower.Name != newLeader.Name {
										newFollowers = append(newFollowers, follower)
									}
								}
								if status.Leader != nil && (newLeader == nil || status.Leader.Name != newLeader.Name) {
									newFollowers = append(newFollowers, *status.Leader)
								}
								status.Term = &term
								status.Leader = newLeader
								status.Followers = newFollowers
								return true
							}
							return false
						}, func(group *raftv1beta2.RaftPartition) {
							if group.Status.Leader != nil {
								r.events.Eventf(group, "Normal", "LeaderChanged", "replica %d elected leader for term %d", *group.Status.Leader, e.LeaderUpdated.Term)
							} else {
								r.events.Eventf(group, "Normal", "TermChanged", "Term changed to %d", e.LeaderUpdated.Term)
							}
						})
				case *raftv1.Event_ConfigurationChanged:
					r.recordReplicaEvent(ctx, log, clusterName, e.ConfigurationChanged.GroupID, e.ConfigurationChanged.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							return true
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "ReplicashipChanged", "Replicaship changed")
						})
				case *raftv1.Event_SendSnapshotStarted:
					r.recordReplicaEvent(ctx, log, clusterName, e.SendSnapshotStarted.GroupID, e.SendSnapshotStarted.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							return true
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SendSnapshotStared", "Started sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotStarted.Index, clusterName.Name, e.SendSnapshotStarted.GroupID, e.SendSnapshotStarted.To)
						})
				case *raftv1.Event_SendSnapshotCompleted:
					r.recordReplicaEvent(ctx, log, clusterName, e.SendSnapshotCompleted.GroupID, e.SendSnapshotCompleted.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							return true
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SendSnapshotCompleted", "Completed sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotCompleted.Index, clusterName.Name, e.SendSnapshotCompleted.GroupID, e.SendSnapshotCompleted.To)
						})
				case *raftv1.Event_SendSnapshotAborted:
					r.recordReplicaEvent(ctx, log, clusterName, e.SendSnapshotAborted.GroupID, e.SendSnapshotAborted.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							return true
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SendSnapshotAborted", "Aborted sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotAborted.Index, clusterName.Name, e.SendSnapshotAborted.GroupID, e.SendSnapshotAborted.To)
						})
				case *raftv1.Event_SnapshotReceived:
					index := uint64(e.SnapshotReceived.Index)
					r.recordReplicaEvent(ctx, log, clusterName, e.SnapshotReceived.GroupID, e.SnapshotReceived.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SnapshotReceived", "Snapshot received from %s-%d-%d at index %d",
								clusterName.Name, e.SnapshotReceived.GroupID, e.SnapshotReceived.From, e.SnapshotReceived.Index)
						})
				case *raftv1.Event_SnapshotRecovered:
					index := uint64(e.SnapshotRecovered.Index)
					r.recordReplicaEvent(ctx, log, clusterName, e.SnapshotRecovered.GroupID, e.SnapshotRecovered.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SnapshotRecovered", "Recovered from snapshot at index %d", e.SnapshotRecovered.Index)
						})
				case *raftv1.Event_SnapshotCreated:
					index := uint64(e.SnapshotCreated.Index)
					r.recordReplicaEvent(ctx, log, clusterName, e.SnapshotCreated.GroupID, e.SnapshotCreated.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SnapshotCreated", "Created snapshot at index %d", e.SnapshotCreated.Index)
						})
				case *raftv1.Event_SnapshotCompacted:
					index := uint64(e.SnapshotCompacted.Index)
					r.recordReplicaEvent(ctx, log, clusterName, e.SnapshotCompacted.GroupID, e.SnapshotCompacted.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "SnapshotCompacted", "Compacted snapshot at index %d", e.SnapshotCompacted.Index)
						})
				case *raftv1.Event_LogCompacted:
					r.recordReplicaEvent(ctx, log, clusterName, e.LogCompacted.GroupID, e.LogCompacted.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							return true
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "LogCompacted", "Compacted log at index %d", e.LogCompacted.Index)
						})
				case *raftv1.Event_LogdbCompacted:
					r.recordReplicaEvent(ctx, log, clusterName, e.LogdbCompacted.GroupID, e.LogdbCompacted.MemberID,
						func(status *raftv1beta2.RaftReplicaStatus) bool {
							return true
						}, func(replica *raftv1beta2.RaftReplica) {
							r.events.Eventf(replica, "Normal", "LogCompacted", "Compacted log at index %d", e.LogdbCompacted.Index)
						})
				}
			}
		}
	}()
	return nil
}

func (r *PodReconciler) getReplica(ctx context.Context, log logging.Logger, clusterName types.NamespacedName, groupID raftv1beta2.GroupID, memberID raftv1beta2.MemberID) (*raftv1beta2.RaftReplica, error) {
	options := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftNamespaceKey: clusterName.Namespace,
			raftClusterKey:   clusterName.Name,
			raftGroupIDKey:   strconv.Itoa(int(groupID)),
			raftMemberIDKey:  strconv.Itoa(int(memberID)),
		}),
	}
	replicas := &raftv1beta2.RaftReplicaList{}
	if err := r.client.List(ctx, replicas, options); err != nil {
		log.Error(err)
		return nil, err
	}
	if len(replicas.Items) == 0 {
		log.Warn("No replica found matching the expected group/member identifiers")
		return nil, errors.NewNotFound("member %d not found in group %d", memberID, groupID)
	}
	return &replicas.Items[0], nil
}

func (r *PodReconciler) getPartition(ctx context.Context, log logging.Logger, clusterName types.NamespacedName, groupID raftv1beta2.GroupID) (*raftv1beta2.RaftPartition, error) {
	options := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftNamespaceKey: clusterName.Namespace,
			raftClusterKey:   clusterName.Name,
			raftGroupIDKey:   strconv.Itoa(int(groupID)),
		}),
	}
	partitions := &raftv1beta2.RaftPartitionList{}
	if err := r.client.List(ctx, partitions, options); err != nil {
		log.Error(err)
		return nil, err
	}
	if len(partitions.Items) == 0 {
		log.Warn("No partition found matching the expected group identifiers")
		return nil, errors.NewNotFound("group %d not found in cluster %s", groupID, clusterName.Name)
	}
	return &partitions.Items[0], nil
}

func (r *PodReconciler) recordReplicaEvent(
	ctx context.Context, log logging.Logger, clusterName types.NamespacedName, groupID raftv1.GroupID, memberID raftv1.MemberID,
	updater func(*raftv1beta2.RaftReplicaStatus) bool, recorder func(*raftv1beta2.RaftReplica)) {
	_ = backoff.Retry(func() error {
		return r.tryRecordReplicaEvent(ctx, log.WithFields(
			logging.Uint64("GroupID", uint64(groupID)),
			logging.Uint64("MemberID", uint64(memberID))),
			clusterName, raftv1beta2.GroupID(groupID), raftv1beta2.MemberID(memberID), updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordReplicaEvent(
	ctx context.Context, log logging.Logger, clusterName types.NamespacedName, groupID raftv1beta2.GroupID, memberID raftv1beta2.MemberID,
	updater func(*raftv1beta2.RaftReplicaStatus) bool, recorder func(*raftv1beta2.RaftReplica)) error {
	replica, err := r.getReplica(ctx, log, clusterName, groupID, memberID)
	if err != nil {
		return err
	}

	if updater(&replica.Status) {
		log.Debug("Replica status changed")
		if err := updateStatus(r.client, ctx, replica, log); err != nil {
			return err
		}
		recorder(replica)
	}
	return nil
}

func (r *PodReconciler) recordPartitionEvent(
	ctx context.Context, log logging.Logger, clusterName types.NamespacedName, groupID raftv1.GroupID,
	updater func(status *raftv1beta2.RaftPartitionStatus) bool, recorder func(*raftv1beta2.RaftPartition)) {
	_ = backoff.Retry(func() error {
		return r.tryRecordPartitionEvent(ctx,
			log.WithFields(logging.Uint64("GroupID", uint64(groupID))),
			clusterName, raftv1beta2.GroupID(groupID), updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordPartitionEvent(
	ctx context.Context, log logging.Logger, clusterName types.NamespacedName, groupID raftv1beta2.GroupID,
	updater func(status *raftv1beta2.RaftPartitionStatus) bool, recorder func(*raftv1beta2.RaftPartition)) error {
	partition, err := r.getPartition(ctx, log, clusterName, groupID)
	if err != nil {
		return err
	}

	if updater(&partition.Status) {
		log.Debug("Partition status changed")
		if err := updateStatus(r.client, ctx, partition, log); err != nil {
			return err
		}
		recorder(partition)
	}
	return nil
}

func (r *PodReconciler) unwatch(address string) error {
	r.mu.RLock()
	cancel, ok := r.watchers[address]
	r.mu.RUnlock()
	if ok {
		log.Infof("Cancelling Watch for %s", address)
		cancel()
	}
	return nil
}

var _ reconcile.Reconciler = (*PodReconciler)(nil)
