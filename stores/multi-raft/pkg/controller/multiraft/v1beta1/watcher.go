// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/errors"
	"github.com/atomix/atomix/runtime/pkg/grpc/retry"
	multiraftv1 "github.com/atomix/atomix/stores/multi-raft/pkg/api/v1"
	"github.com/cenkalti/backoff"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sync"
	"time"

	multiraftv1beta1 "github.com/atomix/atomix/stores/multi-raft/pkg/apis/multiraft/v1beta1"
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
			events:   mgr.GetEventRecorderFor("atomix-multi-raft-storage"),
			watchers: make(map[string]context.CancelFunc),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("pod-v1", mgr, options)
	if err != nil {
		return err
	}

	err = controller.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}
	return nil
}

// PodReconciler reconciles a MultiRaftCluster object
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
	store, ok := pod.Annotations[multiRaftStoreKey]
	if !ok {
		return nil
	}

	if !hasFinalizer(pod, multiRaftPodFinalizer) {
		addFinalizer(pod, multiRaftPodFinalizer)
		if err := r.client.Update(ctx, pod); err != nil {
			return err
		}
		return nil
	}

	address := fmt.Sprintf("%s:%d", getPodDNSName(pod.Namespace, store, pod.Name), apiPort)
	storeName := types.NamespacedName{
		Namespace: pod.Namespace,
		Name:      store,
	}
	if err := r.watch(storeName, address); err != nil {
		return err
	}
	return nil
}

func (r *PodReconciler) reconcileDelete(ctx context.Context, pod *corev1.Pod) error {
	store, ok := pod.Annotations[multiRaftStoreKey]
	if !ok {
		return nil
	}

	if !hasFinalizer(pod, multiRaftPodFinalizer) {
		return nil
	}

	address := fmt.Sprintf("%s:%d", getPodDNSName(pod.Namespace, store, pod.Name), apiPort)
	if err := r.unwatch(address); err != nil {
		return err
	}

	removeFinalizer(pod, multiRaftPodFinalizer)
	if err := r.client.Update(ctx, pod); err != nil {
		return err
	}
	return nil
}

func (r *PodReconciler) watch(storeName types.NamespacedName, address string) error {
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

		client := multiraftv1.NewNodeClient(conn)

		request := &multiraftv1.WatchRequest{}
		stream, err := client.Watch(ctx, request)
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
				case *multiraftv1.Event_MemberReady:
					r.recordMemberEvent(ctx, storeName, e.MemberReady.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							if status.State != multiraftv1beta1.RaftMemberReady {
								status.State = multiraftv1beta1.RaftMemberReady
								status.LastUpdated = &timestamp
								return true
							}
							return false
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "StateChanged", "Member is ready")
						})
					r.recordGroupEvent(ctx, storeName, e.MemberReady.GroupID,
						func(status *multiraftv1beta1.RaftGroupStatus) bool {
							memberName := corev1.LocalObjectReference{
								Name: fmt.Sprintf("%s-%d-%d", storeName.Name, e.MemberReady.GroupID, e.MemberReady.MemberID),
							}
							if status.Leader != nil && status.Leader.Name == memberName.Name {
								return false
							}
							for _, follower := range status.Followers {
								if follower.Name == memberName.Name {
									return false
								}
							}
							status.Followers = append(status.Followers, memberName)
							return true
						}, func(group *multiraftv1beta1.RaftGroup) {})
				case *multiraftv1.Event_LeaderUpdated:
					r.recordMemberEvent(ctx, storeName, e.LeaderUpdated.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							term := uint64(e.LeaderUpdated.Term)
							if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
								role := multiraftv1beta1.RaftFollower
								if e.LeaderUpdated.Leader == 0 {
									status.Leader = nil
								} else {
									status.Leader = &corev1.LocalObjectReference{
										Name: fmt.Sprintf("%s-%d-%d", storeName.Name, e.LeaderUpdated.GroupID, e.LeaderUpdated.Leader),
									}
									if e.LeaderUpdated.Leader == e.LeaderUpdated.MemberID {
										role = multiraftv1beta1.RaftLeader
									}
								}
								status.Term = &term
								status.Role = &role
								status.LastUpdated = &timestamp
								return true
							}
							return false
						}, func(member *multiraftv1beta1.RaftMember) {
							if member.Status.Role != nil && *member.Status.Role == multiraftv1beta1.RaftLeader {
								r.events.Eventf(member, "Normal", "ElectedLeader", "Elected leader for term %d", e.LeaderUpdated.Term)
							}
						})
					r.recordGroupEvent(ctx, storeName, e.LeaderUpdated.MemberEvent.GroupID,
						func(status *multiraftv1beta1.RaftGroupStatus) bool {
							term := uint64(e.LeaderUpdated.Term)
							if status.Term == nil || *status.Term < term || (*status.Term == term && status.Leader == nil && e.LeaderUpdated.Leader != 0) {
								var leader *corev1.LocalObjectReference
								if e.LeaderUpdated.Leader != 0 {
									leader = &corev1.LocalObjectReference{
										Name: fmt.Sprintf("%s-%d-%d", storeName.Name, e.LeaderUpdated.GroupID, e.LeaderUpdated.Leader),
									}
								}
								if status.Leader != nil && (leader == nil || status.Leader.Name != leader.Name) {
									status.Followers = append(status.Followers, *status.Leader)
								}
								var followers []corev1.LocalObjectReference
								for _, follower := range status.Followers {
									if leader == nil || follower.Name != leader.Name {
										followers = append(followers, follower)
									}
								}
								status.Term = &term
								status.Leader = leader
								status.Followers = followers
								return true
							}
							return false
						}, func(group *multiraftv1beta1.RaftGroup) {
							if group.Status.Leader != nil {
								r.events.Eventf(group, "Normal", "LeaderChanged", "%s elected leader for term %d", group.Status.Leader.Name, e.LeaderUpdated.Term)
							} else {
								r.events.Eventf(group, "Normal", "TermChanged", "Term changed to %d", e.LeaderUpdated.Term)
							}
						})
				case *multiraftv1.Event_MembershipChanged:
					r.recordMemberEvent(ctx, storeName, e.MembershipChanged.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							return true
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "MembershipChanged", "Membership changed")
						})
				case *multiraftv1.Event_SendSnapshotStarted:
					r.recordMemberEvent(ctx, storeName, e.SendSnapshotStarted.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							return true
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SendSnapshotStared", "Started sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotStarted.Index, storeName.Name, e.SendSnapshotStarted.GroupID, e.SendSnapshotStarted.To)
						})
				case *multiraftv1.Event_SendSnapshotCompleted:
					r.recordMemberEvent(ctx, storeName, e.SendSnapshotCompleted.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							return true
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SendSnapshotCompleted", "Completed sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotCompleted.Index, storeName.Name, e.SendSnapshotCompleted.GroupID, e.SendSnapshotCompleted.To)
						})
				case *multiraftv1.Event_SendSnapshotAborted:
					r.recordMemberEvent(ctx, storeName, e.SendSnapshotAborted.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							return true
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SendSnapshotAborted", "Aborted sending snapshot at index %d to %s-%d-%d",
								e.SendSnapshotAborted.Index, storeName.Name, e.SendSnapshotAborted.GroupID, e.SendSnapshotAborted.To)
						})
				case *multiraftv1.Event_SnapshotReceived:
					index := uint64(e.SnapshotReceived.Index)
					r.recordMemberEvent(ctx, storeName, e.SnapshotReceived.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotReceived", "Snapshot received from %s-%d-%d at index %d",
								storeName.Name, e.SnapshotReceived.GroupID, e.SnapshotReceived.From, e.SnapshotReceived.Index)
						})
				case *multiraftv1.Event_SnapshotRecovered:
					index := uint64(e.SnapshotRecovered.Index)
					r.recordMemberEvent(ctx, storeName, e.SnapshotRecovered.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotRecovered", "Recovered from snapshot at index %d", e.SnapshotRecovered.Index)
						})
				case *multiraftv1.Event_SnapshotCreated:
					index := uint64(e.SnapshotCreated.Index)
					r.recordMemberEvent(ctx, storeName, e.SnapshotCreated.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotCreated", "Created snapshot at index %d", e.SnapshotCreated.Index)
						})
				case *multiraftv1.Event_SnapshotCompacted:
					index := uint64(e.SnapshotCompacted.Index)
					r.recordMemberEvent(ctx, storeName, e.SnapshotCompacted.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							if index > 0 && (status.LastSnapshotIndex == nil || index > *status.LastSnapshotIndex) {
								status.LastUpdated = &timestamp
								status.LastSnapshotTime = &timestamp
								status.LastSnapshotIndex = &index
								return true
							}
							return false
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "SnapshotCompacted", "Compacted snapshot at index %d", e.SnapshotCompacted.Index)
						})
				case *multiraftv1.Event_LogCompacted:
					r.recordMemberEvent(ctx, storeName, e.LogCompacted.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							return true
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "LogCompacted", "Compacted log at index %d", e.LogCompacted.Index)
						})
				case *multiraftv1.Event_LogdbCompacted:
					r.recordMemberEvent(ctx, storeName, e.LogdbCompacted.MemberEvent,
						func(status *multiraftv1beta1.RaftMemberStatus) bool {
							return true
						}, func(member *multiraftv1beta1.RaftMember) {
							r.events.Eventf(member, "Normal", "LogCompacted", "Compacted log at index %d", e.LogdbCompacted.Index)
						})
				}
			}
		}
	}()
	return nil
}

func (r *PodReconciler) recordMemberEvent(ctx context.Context,
	storeName types.NamespacedName, event multiraftv1.MemberEvent,
	updater func(*multiraftv1beta1.RaftMemberStatus) bool, recorder func(*multiraftv1beta1.RaftMember)) {
	memberName := types.NamespacedName{
		Namespace: storeName.Namespace,
		Name:      fmt.Sprintf("%s-%d-%d", storeName.Name, event.GroupID, event.MemberID),
	}
	_ = backoff.Retry(func() error {
		return r.tryRecordMemberEvent(ctx, memberName, updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordMemberEvent(ctx context.Context, memberName types.NamespacedName, updater func(*multiraftv1beta1.RaftMemberStatus) bool, recorder func(*multiraftv1beta1.RaftMember)) error {
	member := &multiraftv1beta1.RaftMember{}
	if err := r.client.Get(ctx, memberName, member); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if updater(&member.Status) {
		if err := r.client.Status().Update(ctx, member); err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		recorder(member)
	}
	return nil
}

func (r *PodReconciler) recordGroupEvent(ctx context.Context,
	storeName types.NamespacedName, groupID multiraftv1.GroupID,
	updater func(status *multiraftv1beta1.RaftGroupStatus) bool, recorder func(*multiraftv1beta1.RaftGroup)) {
	groupName := types.NamespacedName{
		Namespace: storeName.Namespace,
		Name:      fmt.Sprintf("%s-%d", storeName.Name, groupID),
	}
	_ = backoff.Retry(func() error {
		return r.tryRecordGroupEvent(ctx, groupName, updater, recorder)
	}, backoff.NewExponentialBackOff())
}

func (r *PodReconciler) tryRecordGroupEvent(ctx context.Context, groupName types.NamespacedName, updater func(status *multiraftv1beta1.RaftGroupStatus) bool, recorder func(*multiraftv1beta1.RaftGroup)) error {
	group := &multiraftv1beta1.RaftGroup{}
	if err := r.client.Get(ctx, groupName, group); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if updater(&group.Status) {
		if err := r.client.Status().Update(ctx, group); err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		recorder(group)
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

func hasFinalizer(object client.Object, name string) bool {
	for _, finalizer := range object.GetFinalizers() {
		if finalizer == name {
			return true
		}
	}
	return false
}

func addFinalizer(object client.Object, name string) {
	object.SetFinalizers(append(object.GetFinalizers(), name))
}

func removeFinalizer(object client.Object, name string) {
	var finalizers []string
	for _, finalizer := range object.GetFinalizers() {
		if finalizer != name {
			finalizers = append(finalizers, finalizer)
		}
	}
	object.SetFinalizers(finalizers)
}
