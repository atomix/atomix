// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
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

func addRaftPartitionController(mgr manager.Manager) error {
	options := controller.Options{
		Reconciler: &RaftPartitionReconciler{
			client: mgr.GetClient(),
			scheme: mgr.GetScheme(),
			events: mgr.GetEventRecorderFor("atomix-raft-storage"),
		},
		RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond*10, time.Second*5),
	}

	// Create a new controller
	controller, err := controller.New("atomix-raft-partition", mgr, options)
	if err != nil {
		return err
	}

	// Watch for changes to the storage resource and enqueue Stores that reference it
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftPartition{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftMember
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftMember{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta2.RaftPartition{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftCluster
	err = controller.Watch(&source.Kind{Type: &raftv1beta2.RaftCluster{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		partitions := &raftv1beta2.RaftPartitionList{}
		if err := mgr.GetClient().List(context.Background(), partitions, &client.ListOptions{Namespace: object.GetNamespace()}); err != nil {
			return nil
		}

		var requests []reconcile.Request
		for _, partition := range partitions.Items {
			if partition.Spec.Cluster.Name == object.GetName() && getClusterNamespace(&partition, partition.Spec.Cluster) == object.GetNamespace() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: object.GetNamespace(),
						Name:      partition.Name,
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

// RaftPartitionReconciler reconciles a RaftPartition object
type RaftPartitionReconciler struct {
	client client.Client
	scheme *runtime.Scheme
	events record.EventRecorder
}

// Reconcile reads that state of the cluster for a Store object and makes changes based on the state read
// and what is in the Store.Spec
func (r *RaftPartitionReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := log.WithFields(logging.String("RaftPartition", request.NamespacedName.String()))
	log.Debug("Reconciling RaftPartition")

	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, request.NamespacedName, partition); err != nil {
		return reconcile.Result{}, logError(log, err)
	}

	log = log.WithFields(
		logging.Uint64("PartitionID", uint64(partition.Spec.PartitionID)),
		logging.Uint64("ShardID", uint64(partition.Spec.ShardID)))

	if partition.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, log, partition)
	}
	return r.reconcileCreate(ctx, log, partition)
}

func (r *RaftPartitionReconciler) reconcileCreate(ctx context.Context, log logging.Logger, partition *raftv1beta2.RaftPartition) (reconcile.Result, error) {
	if !hasFinalizer(partition, raftPartitionKey) {
		log.Debugf("Adding %s finalizer", raftPartitionKey)
		addFinalizer(partition, raftPartitionKey)
		if err := r.client.Update(ctx, partition); err != nil {
			return reconcile.Result{}, logError(log, err)
		}
		return reconcile.Result{}, nil
	}

	cluster := &raftv1beta2.RaftCluster{}
	clusterName := types.NamespacedName{
		Namespace: getClusterNamespace(partition, partition.Spec.Cluster),
		Name:      partition.Spec.Cluster.Name,
	}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		return reconcile.Result{}, logError(log, err)
	}

	if ok, err := r.reconcileMembers(ctx, log, cluster, partition); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileStatus(ctx, log, partition); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *RaftPartitionReconciler) reconcileMembers(ctx context.Context, log logging.Logger, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition) (bool, error) {
	// Iterate through partition members and ensure member statuses have been added to the partition status
	memberStatuses := partition.Status.MemberStatuses
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		memberID := raftv1beta2.MemberID(ordinal)
		memberName := fmt.Sprintf("%s-%d", partition.Name, memberID)

		hasMember := false
		for _, memberStatus := range memberStatuses {
			if memberStatus.Name == memberName {
				hasMember = true
				break
			}
		}

		if !hasMember {
			partition.Status.LastReplicaID++
			memberStatus := raftv1beta2.RaftPartitionMemberStatus{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: memberName,
				},
				MemberID:  memberID,
				ReplicaID: partition.Status.LastReplicaID,
			}
			partition.Status.MemberStatuses = append(partition.Status.MemberStatuses, memberStatus)
			log.Infow("Initializing member status",
				logging.Uint64("MemberID", uint64(memberStatus.MemberID)),
				logging.Uint64("ReplicaID", uint64(memberStatus.ReplicaID)))
			if err := r.client.Status().Update(ctx, partition); err != nil {
				return false, logError(log, err)
			}
			return true, nil
		}
	}

	// Iterate through partition members and reconcile them, returning in the event of any state change
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		if ok, err := r.reconcileMember(ctx, log, cluster, partition, raftv1beta2.MemberID(ordinal)); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

func (r *RaftPartitionReconciler) reconcileMember(ctx context.Context, log logging.Logger, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID) (bool, error) {
	podName := types.NamespacedName{
		Namespace: getClusterNamespace(partition, partition.Spec.Cluster),
		Name:      getMemberPodName(cluster, partition, memberID),
	}
	log = log.WithFields(
		logging.Uint64("MemberID", uint64(memberID)),
		logging.Stringer("Pod", podName))

	log.Debug("Reconciling RaftMember")
	memberName := types.NamespacedName{
		Namespace: partition.Namespace,
		Name:      fmt.Sprintf("%s-%d", partition.Name, memberID),
	}
	member := &raftv1beta2.RaftMember{}
	if err := r.client.Get(ctx, memberName, member); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return false, err
		}

		pod := &corev1.Pod{}
		if err := r.client.Get(ctx, podName, pod); err != nil {
			if !k8serrors.IsNotFound(err) {
				log.Error(err)
				return false, err
			}

			// If the member Pod was deleted, mark the member status deleted
			for i, memberRef := range partition.Status.MemberStatuses {
				if memberRef.Name == member.Name {
					if !memberRef.Deleted {
						memberRef.Deleted = true
						partition.Status.MemberStatuses[i] = memberRef
						log.Infow("Pod not found; updating partition status",
							logging.Uint64("ReplicaID", uint64(memberRef.ReplicaID)))
						if err := r.client.Status().Update(ctx, partition); err != nil {
							return false, logError(log, err)
						}
					}
					break
				}
			}
			return false, nil
		}

		// Lookup the Raft node ID for the member in the partition status
		var raftReplicaID *raftv1beta2.ReplicaID
		boostrapPolicy := raftv1beta2.RaftBootstrap
		for i, memberStatus := range partition.Status.MemberStatuses {
			if memberStatus.Name == memberName.Name {
				// If this member is marked 'deleted', store the new Raft node ID and reset the flags
				if memberStatus.Deleted {
					partition.Status.LastReplicaID++
					memberStatus.ReplicaID = partition.Status.LastReplicaID
					memberStatus.Bootstrapped = true
					memberStatus.Deleted = false
					partition.Status.MemberStatuses[i] = memberStatus
					log.Infow("Pod found; assigning new replica ID to member",
						logging.Uint64("ReplicaID", uint64(memberStatus.ReplicaID)))
					if err := r.client.Status().Update(ctx, partition); err != nil {
						return false, logError(log, err)
					}
					return true, nil
				}

				// If the member has already been bootstrapped, configure the member to join the Raft cluster
				if memberStatus.Bootstrapped {
					boostrapPolicy = raftv1beta2.RaftJoin
				} else {
					boostrapPolicy = raftv1beta2.RaftBootstrap
				}

				raftReplicaID = &memberStatus.ReplicaID
				break
			}
		}

		// If the member is not found in the partition status, add a new member status
		if raftReplicaID == nil {
			partition.Status.LastReplicaID++
			memberStatus := raftv1beta2.RaftPartitionMemberStatus{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: memberName.Name,
				},
				MemberID:  memberID,
				ReplicaID: partition.Status.LastReplicaID,
			}
			partition.Status.MemberStatuses = append(partition.Status.MemberStatuses, memberStatus)
			log.Infow("Initializing member status",
				logging.Uint64("ReplicaID", uint64(memberStatus.ReplicaID)))
			if err := r.client.Status().Update(ctx, partition); err != nil {
				return false, logError(log, err)
			}
			return true, nil
		}

		// Get the current configuration from the partition member statuses
		peers := make([]raftv1beta2.RaftMemberReference, 0, len(partition.Status.MemberStatuses))
		for _, memberStatus := range partition.Status.MemberStatuses {
			peers = append(peers, raftv1beta2.RaftMemberReference{
				Pod: corev1.LocalObjectReference{
					Name: getMemberPodName(cluster, partition, memberStatus.MemberID),
				},
				MemberID:  memberStatus.MemberID,
				ReplicaID: memberStatus.ReplicaID,
			})
		}

		// Create the new member
		member = &raftv1beta2.RaftMember{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   memberName.Namespace,
				Name:        memberName.Name,
				Labels:      newMemberLabels(cluster, partition, memberID, *raftReplicaID),
				Annotations: newMemberAnnotations(cluster, partition, memberID, *raftReplicaID),
			},
			Spec: raftv1beta2.RaftMemberSpec{
				Cluster:   partition.Spec.Cluster,
				ShardID:   partition.Spec.ShardID,
				MemberID:  memberID,
				ReplicaID: *raftReplicaID,
				Pod: corev1.LocalObjectReference{
					Name: pod.Name,
				},
				Type:            raftv1beta2.RaftVoter,
				BootstrapPolicy: boostrapPolicy,
				Config: raftv1beta2.RaftMemberConfig{
					RaftConfig: partition.Spec.RaftConfig,
					Peers:      peers,
				},
			},
		}
		log.Infow("Creating RaftMember",
			logging.Uint64("ReplicaID", uint64(member.Spec.ReplicaID)))
		addFinalizer(member, raftPartitionKey)
		if err := controllerutil.SetControllerReference(partition, member, r.scheme); err != nil {
			log.Error(err)
			return false, err
		}
		if err := controllerutil.SetOwnerReference(pod, member, r.scheme); err != nil {
			log.Error(err)
			return false, err
		}
		if err := r.client.Create(ctx, member); err != nil {
			return false, logError(log, err)
		}
		return true, nil
	}

	// If the member is being deleted, mark the member's status as 'deleted' in the partition
	// statuses and remove the finalizer.
	if member.DeletionTimestamp != nil && hasFinalizer(member, raftPartitionKey) {
		for i, memberRef := range partition.Status.MemberStatuses {
			if memberRef.Name == member.Name {
				if !memberRef.Deleted {
					memberRef.Deleted = true
					partition.Status.MemberStatuses[i] = memberRef
					log.Infow("RaftMember deleted; updating partition status",
						logging.Uint64("ReplicaID", uint64(memberRef.ReplicaID)))
					if err := r.client.Status().Update(ctx, partition); err != nil {
						return false, logError(log, err)
					}
				}
				break
			}
		}

		log.Debugf("Removing %s finalizer", raftPartitionKey)
		removeFinalizer(member, raftPartitionKey)
		if err := r.client.Update(ctx, member); err != nil {
			return false, logError(log, err)
		}
		return true, nil
	}
	return false, nil
}

func (r *RaftPartitionReconciler) reconcileStatus(ctx context.Context, log logging.Logger, partition *raftv1beta2.RaftPartition) (bool, error) {
	state := raftv1beta2.RaftPartitionReady
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		memberName := types.NamespacedName{
			Namespace: partition.Namespace,
			Name:      fmt.Sprintf("%s-%d", partition.Name, ordinal),
		}
		member := &raftv1beta2.RaftMember{}
		if err := r.client.Get(ctx, memberName, member); err != nil {
			return false, logError(log, err)
		}
		if member.Status.State == raftv1beta2.RaftMemberNotReady {
			state = raftv1beta2.RaftPartitionNotReady
		}
	}

	if partition.Status.State != state {
		partition.Status.State = state
		log.Infow("Partition status changed",
			logging.String("Status", string(state)))
		if err := r.client.Status().Update(ctx, partition); err != nil {
			return false, logError(log, err)
		}
		return true, nil
	}
	return false, nil
}

func (r *RaftPartitionReconciler) reconcileDelete(ctx context.Context, log logging.Logger, partition *raftv1beta2.RaftPartition) (reconcile.Result, error) {
	if !hasFinalizer(partition, raftPartitionKey) {
		return reconcile.Result{}, nil
	}

	options := &client.ListOptions{
		Namespace: partition.Namespace,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			raftPartitionKey: strconv.Itoa(int(partition.Spec.PartitionID)),
		}),
	}
	members := &raftv1beta2.RaftMemberList{}
	if err := r.client.List(ctx, members, options); err != nil {
		return reconcile.Result{}, logError(log, err)
	}

	for _, member := range members.Items {
		if hasFinalizer(&member, raftPartitionKey) {
			memberName := types.NamespacedName{
				Namespace: member.Namespace,
				Name:      member.Name,
			}
			log.WithFields(logging.Stringer("RaftMember", memberName)).
				Debugf("Removing %s finalizer", raftPartitionKey)
			removeFinalizer(&member, raftPartitionKey)
			if err := r.client.Update(ctx, &member); err != nil {
				return reconcile.Result{}, logError(log, err)
			}
		}
	}

	log.Debugf("Removing %s finalizer", raftPartitionKey)
	removeFinalizer(partition, raftPartitionKey)
	if err := r.client.Update(ctx, partition); err != nil {
		return reconcile.Result{}, logError(log, err)
	}
	return reconcile.Result{}, nil
}

var _ reconcile.Reconciler = (*RaftPartitionReconciler)(nil)
