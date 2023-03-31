// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta3

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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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

	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
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
			watchers: make(map[types.NamespacedName]context.CancelFunc),
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
	watchers map[types.NamespacedName]context.CancelFunc
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
	if err := r.watch(ctx, log, pod); err != nil {
		return err
	}
	return nil
}

func (r *PodReconciler) reconcileDelete(ctx context.Context, log logging.Logger, pod *corev1.Pod) error {
	if !hasFinalizer(pod, podKey) {
		return nil
	}
	if err := r.unwatch(pod); err != nil {
		return err
	}
	log.Infof("Removing %s finalizer", podKey)
	removeFinalizer(pod, podKey)
	return update(r.client, ctx, pod, log)
}

func (r *PodReconciler) watch(ctx context.Context, log logging.Logger, pod *corev1.Pod) error {
	podName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.watchers[podName]; ok {
		return nil
	}

	address := fmt.Sprintf("%s:%d", getDNSName(pod.Namespace, pod.Annotations[raftClusterKey], pod.Name), apiPort)
	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptors.ErrorHandlingUnaryClientInterceptor()),
		grpc.WithStreamInterceptor(interceptors.ErrorHandlingStreamClientInterceptor()))
	if err != nil {
		log.Warn(err)
		return err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	r.watchers[podName] = cancel

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.watchers, podName)
			r.mu.Unlock()
		}()
		for {
			if err := r.tryWatch(streamCtx, log, pod, conn); err != nil {
				if errors.IsCanceled(err) {
					log.Debug("Watch canceled")
					return
				}
				log.Warn(err)
			}
		}
	}()
	return nil
}

func (r *PodReconciler) tryWatch(ctx context.Context, log logging.Logger, pod *corev1.Pod, conn *grpc.ClientConn) error {
	defer func() {
		timestamp := metav1.Now()
		err := r.updateReplicas(ctx, log, pod, func(status *raftv1beta3.RaftReplicaStatus) bool {
			if status.State != raftv1beta3.RaftReplicaPending {
				status.State = raftv1beta3.RaftReplicaPending
				status.LastUpdated = &timestamp
				return true
			}
			return false
		})
		if err != nil {
			log.Error(err)
		}
	}()

	node := raftv1.NewNodeClient(conn)
	return backoff.Retry(func() error {
		stream, err := node.Watch(ctx, &raftv1.WatchRequest{})
		if err != nil {
			if errors.IsCanceled(err) {
				return backoff.Permanent(err)
			}
			if !errors.IsUnavailable(err) {
				log.Warn(err)
			}
			return err
		}

		for {
			event, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					log.Warn(err)
				}
				return nil
			}

			log.Debugw("Received event", logging.Stringer("Event", event))
			timestamp := metav1.NewTime(event.Timestamp)
			switch e := event.Event.(type) {
			case *raftv1.Event_MemberReady:
				r.recordReplicaEvent(ctx, log, pod, e.MemberReady.GroupID, e.MemberReady.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						if status.State != raftv1beta3.RaftReplicaReady {
							status.State = raftv1beta3.RaftReplicaReady
							status.LastUpdated = &timestamp
							return true
						}
						return false
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "StateChanged", "Replica is ready")
					})
				r.recordPartitionEvent(ctx, log, pod, e.MemberReady.GroupID,
					func(status *raftv1beta3.RaftPartitionStatus) bool {
						replica, err := r.getReplica(ctx, log, pod, raftv1beta3.GroupID(e.MemberReady.GroupID), raftv1beta3.MemberID(e.MemberReady.MemberID))
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
					}, func(group *raftv1beta3.RaftPartition) {})
			case *raftv1.Event_LeaderUpdated:
				r.recordReplicaEvent(ctx, log, pod, e.LeaderUpdated.GroupID, e.LeaderUpdated.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						term := int64(e.LeaderUpdated.Term)
						if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
							role := raftv1beta3.RaftFollower
							if e.LeaderUpdated.Leader == 0 {
								status.Leader = nil
							} else {
								leader, err := r.getReplica(ctx, log, pod, raftv1beta3.GroupID(e.LeaderUpdated.GroupID), raftv1beta3.MemberID(e.LeaderUpdated.Leader))
								if err != nil {
									return false
								}
								status.Leader = &corev1.LocalObjectReference{
									Name: leader.Name,
								}
								if e.LeaderUpdated.Leader == e.LeaderUpdated.MemberID {
									role = raftv1beta3.RaftLeader
								}
							}
							status.Term = &term
							status.Role = &role
							status.LastUpdated = &timestamp
							return true
						}
						return false
					}, func(replica *raftv1beta3.RaftReplica) {
						if replica.Status.Role != nil && *replica.Status.Role == raftv1beta3.RaftLeader {
							r.events.Eventf(replica, "Normal", "ElectedLeader", "Elected leader for term %d", e.LeaderUpdated.Term)
						}
					})
				r.recordPartitionEvent(ctx, log, pod, e.LeaderUpdated.GroupID,
					func(status *raftv1beta3.RaftPartitionStatus) bool {
						term := int64(e.LeaderUpdated.Term)
						if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
							var newLeader *corev1.LocalObjectReference
							if e.LeaderUpdated.Leader != 0 {
								leader, err := r.getReplica(ctx, log, pod, raftv1beta3.GroupID(e.LeaderUpdated.GroupID), raftv1beta3.MemberID(e.LeaderUpdated.Leader))
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
					}, func(group *raftv1beta3.RaftPartition) {
						if group.Status.Leader != nil {
							r.events.Eventf(group, "Normal", "LeaderChanged", "replica %d elected leader for term %d", *group.Status.Leader, e.LeaderUpdated.Term)
						} else {
							r.events.Eventf(group, "Normal", "TermChanged", "Term changed to %d", e.LeaderUpdated.Term)
						}
					})
			case *raftv1.Event_ConfigurationChanged:
				r.recordReplicaEvent(ctx, log, pod, e.ConfigurationChanged.GroupID, e.ConfigurationChanged.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						return true
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "ReplicashipChanged", "Replicaship changed")
					})
			case *raftv1.Event_SendSnapshotStarted:
				r.recordReplicaEvent(ctx, log, pod, e.SendSnapshotStarted.GroupID, e.SendSnapshotStarted.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						return true
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SendSnapshotStared", "Started sending snapshot at index %d to %s-%d-%d",
							e.SendSnapshotStarted.Index, pod.Name, e.SendSnapshotStarted.GroupID, e.SendSnapshotStarted.To)
					})
			case *raftv1.Event_SendSnapshotCompleted:
				r.recordReplicaEvent(ctx, log, pod, e.SendSnapshotCompleted.GroupID, e.SendSnapshotCompleted.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						return true
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SendSnapshotCompleted", "Completed sending snapshot at index %d to %s-%d-%d",
							e.SendSnapshotCompleted.Index, pod.Name, e.SendSnapshotCompleted.GroupID, e.SendSnapshotCompleted.To)
					})
			case *raftv1.Event_SendSnapshotAborted:
				r.recordReplicaEvent(ctx, log, pod, e.SendSnapshotAborted.GroupID, e.SendSnapshotAborted.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						return true
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SendSnapshotAborted", "Aborted sending snapshot at index %d to %s-%d-%d",
							e.SendSnapshotAborted.Index, pod.Name, e.SendSnapshotAborted.GroupID, e.SendSnapshotAborted.To)
					})
			case *raftv1.Event_SnapshotReceived:
				index := int64(e.SnapshotReceived.Index)
				r.recordReplicaEvent(ctx, log, pod, e.SnapshotReceived.GroupID, e.SnapshotReceived.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
							status.LastUpdated = &timestamp
							status.LastSnapshotTime = &timestamp
							status.LastSnapshotIndex = &index
							return true
						}
						return false
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SnapshotReceived", "Snapshot received from %s-%d-%d at index %d",
							pod.Name, e.SnapshotReceived.GroupID, e.SnapshotReceived.From, e.SnapshotReceived.Index)
					})
			case *raftv1.Event_SnapshotRecovered:
				index := int64(e.SnapshotRecovered.Index)
				r.recordReplicaEvent(ctx, log, pod, e.SnapshotRecovered.GroupID, e.SnapshotRecovered.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
							status.LastUpdated = &timestamp
							status.LastSnapshotTime = &timestamp
							status.LastSnapshotIndex = &index
							return true
						}
						return false
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SnapshotRecovered", "Recovered from snapshot at index %d", e.SnapshotRecovered.Index)
					})
			case *raftv1.Event_SnapshotCreated:
				index := int64(e.SnapshotCreated.Index)
				r.recordReplicaEvent(ctx, log, pod, e.SnapshotCreated.GroupID, e.SnapshotCreated.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
							status.LastUpdated = &timestamp
							status.LastSnapshotTime = &timestamp
							status.LastSnapshotIndex = &index
							return true
						}
						return false
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SnapshotCreated", "Created snapshot at index %d", e.SnapshotCreated.Index)
					})
			case *raftv1.Event_SnapshotCompacted:
				index := int64(e.SnapshotCompacted.Index)
				r.recordReplicaEvent(ctx, log, pod, e.SnapshotCompacted.GroupID, e.SnapshotCompacted.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
							status.LastUpdated = &timestamp
							status.LastSnapshotTime = &timestamp
							status.LastSnapshotIndex = &index
							return true
						}
						return false
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "SnapshotCompacted", "Compacted snapshot at index %d", e.SnapshotCompacted.Index)
					})
			case *raftv1.Event_LogCompacted:
				r.recordReplicaEvent(ctx, log, pod, e.LogCompacted.GroupID, e.LogCompacted.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						return true
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "LogCompacted", "Compacted log at index %d", e.LogCompacted.Index)
					})
			case *raftv1.Event_LogdbCompacted:
				r.recordReplicaEvent(ctx, log, pod, e.LogdbCompacted.GroupID, e.LogdbCompacted.MemberID,
					func(status *raftv1beta3.RaftReplicaStatus) bool {
						return true
					}, func(replica *raftv1beta3.RaftReplica) {
						r.events.Eventf(replica, "Normal", "LogCompacted", "Compacted log at index %d", e.LogdbCompacted.Index)
					})
			}
		}
	}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
}

func (r *PodReconciler) updateReplicas(ctx context.Context, log logging.Logger, pod *corev1.Pod, updater func(status *raftv1beta3.RaftReplicaStatus) bool) error {
	options := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftNamespaceKey: pod.Namespace,
			raftClusterKey:   pod.Annotations[raftClusterKey],
			podKey:           pod.Name,
		}),
	}
	replicas := &raftv1beta3.RaftReplicaList{}
	if err := r.client.List(ctx, replicas, options); err != nil {
		log.Error(err)
		return err
	}

	for _, replica := range replicas.Items {
		name := types.NamespacedName{
			Namespace: replica.Namespace,
			Name:      replica.Name,
		}
		if err := r.updateReplica(ctx, log, name, updater); err != nil {
			return err
		}
	}
	return nil
}

func (r *PodReconciler) updateReplica(ctx context.Context, log logging.Logger, name types.NamespacedName, updater func(status *raftv1beta3.RaftReplicaStatus) bool) error {
	return backoff.Retry(func() error {
		return r.tryUpdateReplica(ctx, log, name, updater)
	}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
}

func (r *PodReconciler) tryUpdateReplica(ctx context.Context, log logging.Logger, name types.NamespacedName, updater func(status *raftv1beta3.RaftReplicaStatus) bool) error {
	replica := &raftv1beta3.RaftReplica{}
	if err := r.client.Get(ctx, name, replica); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		log.Warn(err)
		return err
	}
	if updater(&replica.Status) {
		if err := r.client.Status().Update(ctx, replica); err != nil {
			log.Warn(err)
			return err
		}
	}
	return nil
}

func (r *PodReconciler) getReplica(ctx context.Context, log logging.Logger, pod *corev1.Pod, groupID raftv1beta3.GroupID, memberID raftv1beta3.MemberID) (*raftv1beta3.RaftReplica, error) {
	options := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftNamespaceKey: pod.Namespace,
			raftClusterKey:   pod.Annotations[raftClusterKey],
			raftGroupIDKey:   strconv.Itoa(int(groupID)),
			raftMemberIDKey:  strconv.Itoa(int(memberID)),
		}),
	}
	replicas := &raftv1beta3.RaftReplicaList{}
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

func (r *PodReconciler) getPartition(ctx context.Context, log logging.Logger, pod *corev1.Pod, groupID raftv1beta3.GroupID) (*raftv1beta3.RaftPartition, error) {
	options := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftNamespaceKey: pod.Namespace,
			raftClusterKey:   pod.Annotations[raftClusterKey],
			raftGroupIDKey:   strconv.Itoa(int(groupID)),
		}),
	}
	partitions := &raftv1beta3.RaftPartitionList{}
	if err := r.client.List(ctx, partitions, options); err != nil {
		log.Error(err)
		return nil, err
	}
	if len(partitions.Items) == 0 {
		log.Warn("No partition found matching the expected group identifiers")
		return nil, errors.NewNotFound("group %d not found in cluster %s", groupID, pod.Annotations[raftClusterKey])
	}
	return &partitions.Items[0], nil
}

func (r *PodReconciler) recordReplicaEvent(
	ctx context.Context, log logging.Logger, pod *corev1.Pod, groupID raftv1.GroupID, memberID raftv1.MemberID,
	updater func(*raftv1beta3.RaftReplicaStatus) bool, recorder func(*raftv1beta3.RaftReplica)) {
	_ = backoff.Retry(func() error {
		return r.tryRecordReplicaEvent(ctx, log.WithFields(
			logging.Int64("GroupID", int64(groupID)),
			logging.Int64("MemberID", int64(memberID))),
			pod, raftv1beta3.GroupID(groupID), raftv1beta3.MemberID(memberID), updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordReplicaEvent(
	ctx context.Context, log logging.Logger, pod *corev1.Pod, groupID raftv1beta3.GroupID, memberID raftv1beta3.MemberID,
	updater func(*raftv1beta3.RaftReplicaStatus) bool, recorder func(*raftv1beta3.RaftReplica)) error {
	replica, err := r.getReplica(ctx, log, pod, groupID, memberID)
	if err != nil {
		// If the replica is not found, skip recording the event
		if errors.IsCanceled(err) || errors.IsNotFound(err) {
			return nil
		}
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
	ctx context.Context, log logging.Logger, pod *corev1.Pod, groupID raftv1.GroupID,
	updater func(status *raftv1beta3.RaftPartitionStatus) bool, recorder func(*raftv1beta3.RaftPartition)) {
	_ = backoff.Retry(func() error {
		return r.tryRecordPartitionEvent(ctx,
			log.WithFields(logging.Int64("GroupID", int64(groupID))),
			pod, raftv1beta3.GroupID(groupID), updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordPartitionEvent(
	ctx context.Context, log logging.Logger, pod *corev1.Pod, groupID raftv1beta3.GroupID,
	updater func(status *raftv1beta3.RaftPartitionStatus) bool, recorder func(*raftv1beta3.RaftPartition)) error {
	partition, err := r.getPartition(ctx, log, pod, groupID)
	if err != nil {
		// If the partition is not found, skip recording the event
		if errors.IsCanceled(err) || errors.IsNotFound(err) {
			return nil
		}
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

func (r *PodReconciler) unwatch(pod *corev1.Pod) error {
	podName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}
	r.mu.RLock()
	cancel, ok := r.watchers[podName]
	r.mu.RUnlock()
	if ok {
		log.Infof("Cancelling Watch for %s", podName)
		cancel()
	}
	return nil
}

var _ reconcile.Reconciler = (*PodReconciler)(nil)
