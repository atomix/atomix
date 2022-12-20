// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/grpc/retry"
	raftv1 "github.com/atomix/atomix/stores/raft/pkg/api/v1"
	"github.com/cenkalti/backoff"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
	log.Info("Reconcile Pod")
	pod := &corev1.Pod{}
	err := r.client.Get(ctx, request.NamespacedName, pod)
	if err != nil {
		log.Error(err, "Reconcile Pod")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if pod.DeletionTimestamp != nil {
		if err := r.reconcileDelete(ctx, pod); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	if err := r.reconcileCreate(ctx, pod); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *PodReconciler) reconcileCreate(ctx context.Context, pod *corev1.Pod) error {
	cluster, ok := pod.Annotations[raftClusterKey]
	if !ok {
		return nil
	}

	if !hasFinalizer(pod, podKey) {
		addFinalizer(pod, podKey)
		if err := r.client.Update(ctx, pod); err != nil {
			return err
		}
		return nil
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	clusterName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      cluster,
	}
	if err := r.watch(clusterName, address); err != nil {
		return err
	}
	return nil
}

func (r *PodReconciler) reconcileDelete(ctx context.Context, pod *corev1.Pod) error {
	if !hasFinalizer(pod, podKey) {
		return nil
	}

	address := fmt.Sprintf("%s:%d", pod.Status.PodIP, apiPort)
	if err := r.unwatch(address); err != nil {
		return err
	}

	removeFinalizer(pod, podKey)
	if err := r.client.Update(ctx, pod); err != nil {
		return err
	}
	return nil
}

func (r *PodReconciler) watch(clusterName types.NamespacedName, address string) error {
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

		log.Infof("Creating new Watch for %s", address)
		conn, err := grpc.Dial(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStreamInterceptor(retry.RetryingStreamClientInterceptor(retry.WithRetryOn(codes.Unavailable))))
		if err != nil {
			log.Error(err)
			return
		}

		node := raftv1.NewNodeClient(conn)

		request := &raftv1.WatchRequest{}
		stream, err := node.Watch(ctx, request)
		if err != nil {
			log.Error(err)
			return
		}

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				log.Debugf("Watch for %s complete", address)
				return
			}
			if err != nil {
				err = errors.FromProto(err)
				if errors.IsCanceled(err) {
					log.Warnf("Watch for %s canceled", address)
					return
				}
				log.Error(err)
			} else {
				log.Infof("Received event %+v from %s", event, address)
				timestamp := metav1.NewTime(event.Timestamp)
				switch e := event.Event.(type) {
				case *raftv1.Event_ReplicaReady:
					r.recordMemberEvent(ctx, clusterName, e.ReplicaReady.ShardID, e.ReplicaReady.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							if status.State != raftv1beta2.RaftMemberReady {
								status.State = raftv1beta2.RaftMemberReady
								status.LastUpdated = &timestamp
								return true
							}
							return false
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "StateChanged", "Member is ready")
						})
					r.recordPartitionEvent(ctx, clusterName, e.ReplicaReady.ShardID,
						func(status *raftv1beta2.RaftPartitionStatus) bool {
							member, err := r.getMember(ctx, clusterName, raftv1beta2.ShardID(e.ReplicaReady.ShardID), raftv1beta2.ReplicaID(e.ReplicaReady.ReplicaID))
							if err != nil {
								log.Error(err)
								return false
							}
							if status.Leader != nil && *status.Leader == member.Spec.MemberID {
								return false
							}
							for _, follower := range status.Followers {
								if follower == member.Spec.MemberID {
									return false
								}
							}
							status.Followers = append(status.Followers, member.Spec.MemberID)
							return true
						}, func(group *raftv1beta2.RaftPartition) {})
				case *raftv1.Event_LeaderUpdated:
					r.recordMemberEvent(ctx, clusterName, e.LeaderUpdated.ShardID, e.LeaderUpdated.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							term := uint64(e.LeaderUpdated.Term)
							if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
								role := raftv1beta2.RaftFollower
								if e.LeaderUpdated.Leader == 0 {
									status.Leader = nil
								} else {
									leader, err := r.getMember(ctx, clusterName, raftv1beta2.ShardID(e.LeaderUpdated.ShardID), raftv1beta2.ReplicaID(e.LeaderUpdated.Leader))
									if err != nil {
										log.Error(err)
										return false
									}
									status.Leader = &leader.Spec.MemberID
									if e.LeaderUpdated.Leader == e.LeaderUpdated.ReplicaID {
										role = raftv1beta2.RaftLeader
									}
								}
								status.Term = &term
								status.Role = &role
								status.LastUpdated = &timestamp
								return true
							}
							return false
						}, func(member *raftv1beta2.RaftMember) {
							if member.Status.Role != nil && *member.Status.Role == raftv1beta2.RaftLeader {
								r.events.Eventf(member, "Normal", "ElectedLeader", "Elected leader for term %d", e.LeaderUpdated.Term)
							}
						})
					r.recordPartitionEvent(ctx, clusterName, e.LeaderUpdated.ShardID,
						func(status *raftv1beta2.RaftPartitionStatus) bool {
							term := uint64(e.LeaderUpdated.Term)
							if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
								var leaderID *raftv1beta2.MemberID
								if e.LeaderUpdated.Leader != 0 {
									leader, err := r.getMember(ctx, clusterName, raftv1beta2.ShardID(e.LeaderUpdated.ShardID), raftv1beta2.ReplicaID(e.LeaderUpdated.Leader))
									if err != nil {
										return false
									}
									leaderID = &leader.Spec.MemberID
								}
								if status.Leader != nil && (leaderID == nil || *status.Leader != *leaderID) {
									status.Followers = append(status.Followers, *status.Leader)
								}
								var followers []raftv1beta2.MemberID
								for _, follower := range status.Followers {
									if leaderID == nil || follower != *leaderID {
										followers = append(followers, follower)
									}
								}
								status.Term = &term
								status.Leader = leaderID
								status.Followers = followers
								return true
							}
							return false
						}, func(group *raftv1beta2.RaftPartition) {
							if group.Status.Leader != nil {
								r.events.Eventf(group, "Normal", "LeaderChanged", "member %d elected leader for term %d", *group.Status.Leader, e.LeaderUpdated.Term)
							} else {
								r.events.Eventf(group, "Normal", "TermChanged", "Term changed to %d", e.LeaderUpdated.Term)
							}
						})
				case *raftv1.Event_ConfigurationChanged:
					r.recordMemberEvent(ctx, clusterName, e.ConfigurationChanged.ShardID, e.ConfigurationChanged.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							return true
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "MembershipChanged", "Membership changed")
						})
				case *raftv1.Event_SendSnapshotStarted:
					r.recordMemberEvent(ctx, clusterName, e.SendSnapshotStarted.ShardID, e.SendSnapshotStarted.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							return true
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SendSnapshotStared", "Started sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotStarted.Index, clusterName.Name, e.SendSnapshotStarted.ShardID, e.SendSnapshotStarted.To)
						})
				case *raftv1.Event_SendSnapshotCompleted:
					r.recordMemberEvent(ctx, clusterName, e.SendSnapshotCompleted.ShardID, e.SendSnapshotCompleted.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							return true
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SendSnapshotCompleted", "Completed sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotCompleted.Index, clusterName.Name, e.SendSnapshotCompleted.ShardID, e.SendSnapshotCompleted.To)
						})
				case *raftv1.Event_SendSnapshotAborted:
					r.recordMemberEvent(ctx, clusterName, e.SendSnapshotAborted.ShardID, e.SendSnapshotAborted.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							return true
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SendSnapshotAborted", "Aborted sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotAborted.Index, clusterName.Name, e.SendSnapshotAborted.ShardID, e.SendSnapshotAborted.To)
						})
				case *raftv1.Event_SnapshotReceived:
					index := uint64(e.SnapshotReceived.Index)
					r.recordMemberEvent(ctx, clusterName, e.SnapshotReceived.ShardID, e.SnapshotReceived.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotReceived", "Snapshot received from %s-%d-%d at index %d",
								clusterName.Name, e.SnapshotReceived.ShardID, e.SnapshotReceived.From, e.SnapshotReceived.Index)
						})
				case *raftv1.Event_SnapshotRecovered:
					index := uint64(e.SnapshotRecovered.Index)
					r.recordMemberEvent(ctx, clusterName, e.SnapshotRecovered.ShardID, e.SnapshotRecovered.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotRecovered", "Recovered from snapshot at index %d", e.SnapshotRecovered.Index)
						})
				case *raftv1.Event_SnapshotCreated:
					index := uint64(e.SnapshotCreated.Index)
					r.recordMemberEvent(ctx, clusterName, e.SnapshotCreated.ShardID, e.SnapshotCreated.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotCreated", "Created snapshot at index %d", e.SnapshotCreated.Index)
						})
				case *raftv1.Event_SnapshotCompacted:
					index := uint64(e.SnapshotCompacted.Index)
					r.recordMemberEvent(ctx, clusterName, e.SnapshotCompacted.ShardID, e.SnapshotCompacted.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotCompacted", "Compacted snapshot at index %d", e.SnapshotCompacted.Index)
						})
				case *raftv1.Event_LogCompacted:
					r.recordMemberEvent(ctx, clusterName, e.LogCompacted.ShardID, e.LogCompacted.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							return true
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "LogCompacted", "Compacted log at index %d", e.LogCompacted.Index)
						})
				case *raftv1.Event_LogdbCompacted:
					r.recordMemberEvent(ctx, clusterName, e.LogdbCompacted.ShardID, e.LogdbCompacted.ReplicaID,
						func(status *raftv1beta2.RaftMemberStatus) bool {
							return true
						}, func(member *raftv1beta2.RaftMember) {
							r.events.Eventf(member, "Normal", "LogCompacted", "Compacted log at index %d", e.LogdbCompacted.Index)
						})
				}
			}
		}
	}()
	return nil
}

func (r *PodReconciler) getMembers(ctx context.Context, clusterName types.NamespacedName, shardID raftv1beta2.ShardID, replicaID raftv1beta2.ReplicaID) ([]raftv1beta2.RaftMember, error) {
	options := &client.ListOptions{
		Namespace: clusterName.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftClusterKey: clusterName.Name,
			raftShardKey:   strconv.Itoa(int(shardID)),
			raftReplicaKey: strconv.Itoa(int(replicaID)),
		}),
	}
	members := &raftv1beta2.RaftMemberList{}
	if err := r.client.List(ctx, members, options); err != nil {
		log.Error(err)
		return nil, err
	}
	return members.Items, nil
}

func (r *PodReconciler) getMember(ctx context.Context, clusterName types.NamespacedName, shardID raftv1beta2.ShardID, replicaID raftv1beta2.ReplicaID) (*raftv1beta2.RaftMember, error) {
	members, err := r.getMembers(ctx, clusterName, shardID, replicaID)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, errors.NewNotFound("replica %d not found in shard %d", replicaID, shardID)
	}
	return &members[0], nil
}

func (r *PodReconciler) getPartitions(ctx context.Context, clusterName types.NamespacedName, shardID raftv1beta2.ShardID) ([]raftv1beta2.RaftPartition, error) {
	options := &client.ListOptions{
		Namespace: clusterName.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftClusterKey: clusterName.Name,
			raftShardKey:   strconv.Itoa(int(shardID)),
		}),
	}
	partitions := &raftv1beta2.RaftPartitionList{}
	if err := r.client.List(ctx, partitions, options); err != nil {
		log.Error(err)
		return nil, err
	}
	return partitions.Items, nil
}

func (r *PodReconciler) getPartition(ctx context.Context, clusterName types.NamespacedName, shardID raftv1beta2.ShardID) (*raftv1beta2.RaftPartition, error) {
	partitions, err := r.getPartitions(ctx, clusterName, shardID)
	if err != nil {
		return nil, err
	}
	if len(partitions) == 0 {
		return nil, errors.NewNotFound("shard %d not found in cluster %s", shardID, clusterName.Name)
	}
	return &partitions[0], nil
}

func (r *PodReconciler) recordMemberEvent(
	ctx context.Context, clusterName types.NamespacedName, shardID raftv1.ShardID, replicaID raftv1.ReplicaID,
	updater func(*raftv1beta2.RaftMemberStatus) bool, recorder func(*raftv1beta2.RaftMember)) {
	_ = backoff.Retry(func() error {
		return r.tryRecordMemberEvent(ctx, clusterName, raftv1beta2.ShardID(shardID), raftv1beta2.ReplicaID(replicaID), updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordMemberEvent(
	ctx context.Context, clusterName types.NamespacedName, shardID raftv1beta2.ShardID, replicaID raftv1beta2.ReplicaID,
	updater func(*raftv1beta2.RaftMemberStatus) bool, recorder func(*raftv1beta2.RaftMember)) error {
	members, err := r.getMembers(ctx, clusterName, shardID, replicaID)
	if err != nil {
		return err
	}

	for _, member := range members {
		if updater(&member.Status) {
			if err := r.client.Status().Update(ctx, &member); err != nil {
				if k8serrors.IsNotFound(err) {
					return nil
				}
				log.Error(err)
				return err
			}
			recorder(&member)
		}
	}
	return nil
}

func (r *PodReconciler) recordPartitionEvent(
	ctx context.Context, clusterName types.NamespacedName, shardID raftv1.ShardID,
	updater func(status *raftv1beta2.RaftPartitionStatus) bool, recorder func(*raftv1beta2.RaftPartition)) {
	_ = backoff.Retry(func() error {
		return r.tryRecordPartitionEvent(ctx, clusterName, raftv1beta2.ShardID(shardID), updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordPartitionEvent(
	ctx context.Context, clusterName types.NamespacedName, shardID raftv1beta2.ShardID,
	updater func(status *raftv1beta2.RaftPartitionStatus) bool, recorder func(*raftv1beta2.RaftPartition)) error {
	partitions, err := r.getPartitions(ctx, clusterName, shardID)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		if updater(&partition.Status) {
			if err := r.client.Status().Update(ctx, &partition); err != nil {
				if k8serrors.IsNotFound(err) {
					return nil
				}
				log.Error(err)
				return err
			}
			recorder(&partition)
		}
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
