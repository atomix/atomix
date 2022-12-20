// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"context"
	"fmt"
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
	log.Info("Reconcile RaftPartition")
	partition := &raftv1beta2.RaftPartition{}
	if err := r.client.Get(ctx, request.NamespacedName, partition); err != nil {
		log.Error(err, "Reconcile RaftPartition")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if partition.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, partition)
	}
	return r.reconcileCreate(ctx, partition)
}

func (r *RaftPartitionReconciler) reconcileCreate(ctx context.Context, partition *raftv1beta2.RaftPartition) (reconcile.Result, error) {
	if !hasFinalizer(partition, raftPartitionKey) {
		addFinalizer(partition, raftPartitionKey)
		if err := r.client.Update(ctx, partition); err != nil {
			log.Error(err, "Reconcile RaftPartition")
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	cluster := &raftv1beta2.RaftCluster{}
	clusterName := types.NamespacedName{
		Namespace: getClusterNamespace(partition, partition.Spec.Cluster),
		Name:      partition.Spec.Cluster.Name,
	}
	if err := r.client.Get(ctx, clusterName, cluster); err != nil {
		log.Error(err, "Reconcile RaftPartition")
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	if ok, err := r.reconcileMembers(ctx, cluster, partition); err != nil {
		log.Error(err, "Reconcile RaftPartition")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileStatus(ctx, partition); err != nil {
		log.Error(err, "Reconcile RaftPartition")
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *RaftPartitionReconciler) reconcileMembers(ctx context.Context, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition) (bool, error) {
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
			partition.Status.MemberStatuses = append(partition.Status.MemberStatuses, raftv1beta2.RaftPartitionMemberStatus{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: memberName,
				},
				MemberID:  memberID,
				ReplicaID: partition.Status.LastReplicaID,
			})
			if err := r.client.Status().Update(ctx, partition); err != nil {
				log.Error(err, "Reconcile RaftPartition")
				return false, err
			}
			return true, nil
		}
	}

	// Iterate through partition members and reconcile them, returning in the event of any state change
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		if ok, err := r.reconcileMember(ctx, cluster, partition, raftv1beta2.MemberID(ordinal)); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

func (r *RaftPartitionReconciler) reconcileMember(ctx context.Context, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID) (bool, error) {
	memberName := types.NamespacedName{
		Namespace: partition.Namespace,
		Name:      fmt.Sprintf("%s-%d", partition.Name, memberID),
	}
	member := &raftv1beta2.RaftMember{}
	if err := r.client.Get(ctx, memberName, member); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err, "Reconcile RaftPartition")
			return false, err
		}

		// Lookup the Raft node ID for the member in the partition status
		var raftNodeID *raftv1beta2.ReplicaID
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
					if err := r.client.Status().Update(ctx, partition); err != nil {
						log.Error(err, "Reconcile RaftPartition")
						return false, err
					}
					return true, nil
				}

				// If the member has already been bootstrapped, configure the member to join the Raft cluster
				if memberStatus.Bootstrapped {
					boostrapPolicy = raftv1beta2.RaftJoin
				} else {
					boostrapPolicy = raftv1beta2.RaftBootstrap
				}

				raftNodeID = &memberStatus.ReplicaID
				break
			}
		}

		// If the member is not found in the partition status, add a new member status
		if raftNodeID == nil {
			partition.Status.LastReplicaID++
			partition.Status.MemberStatuses = append(partition.Status.MemberStatuses, raftv1beta2.RaftPartitionMemberStatus{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: memberName.Name,
				},
				MemberID:  memberID,
				ReplicaID: partition.Status.LastReplicaID,
			})
			if err := r.client.Status().Update(ctx, partition); err != nil {
				log.Error(err, "Reconcile RaftPartition")
				return false, err
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
				Labels:      newMemberLabels(cluster, partition, memberID, *raftNodeID),
				Annotations: newMemberAnnotations(cluster, partition, memberID, *raftNodeID),
			},
			Spec: raftv1beta2.RaftMemberSpec{
				Cluster:   partition.Spec.Cluster,
				ShardID:   partition.Spec.ShardID,
				MemberID:  memberID,
				ReplicaID: *raftNodeID,
				Pod: corev1.LocalObjectReference{
					Name: getMemberPodName(cluster, partition, memberID),
				},
				Type:            raftv1beta2.RaftVoter,
				BootstrapPolicy: boostrapPolicy,
				Config: raftv1beta2.RaftMemberConfig{
					RaftConfig: partition.Spec.RaftConfig,
					Peers:      peers,
				},
			},
		}
		addFinalizer(member, raftPartitionKey)
		if err := controllerutil.SetControllerReference(partition, member, r.scheme); err != nil {
			log.Error(err, "Reconcile RaftPartition")
			return false, err
		}
		if err := r.client.Create(ctx, member); err != nil {
			log.Error(err, "Reconcile RaftPartition")
			return false, err
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
					if err := r.client.Status().Update(ctx, partition); err != nil {
						log.Error(err, "Reconcile RaftPartition")
						return false, err
					}
				}
				break
			}
		}

		removeFinalizer(member, raftPartitionKey)
		if err := r.client.Update(ctx, member); err != nil {
			log.Error(err, "Reconcile RaftPartition")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *RaftPartitionReconciler) reconcileStatus(ctx context.Context, partition *raftv1beta2.RaftPartition) (bool, error) {
	state := raftv1beta2.RaftPartitionReady
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		memberName := types.NamespacedName{
			Namespace: partition.Namespace,
			Name:      fmt.Sprintf("%s-%d", partition.Name, ordinal),
		}
		member := &raftv1beta2.RaftMember{}
		if err := r.client.Get(ctx, memberName, member); err != nil {
			log.Error(err, "Reconcile RaftPartition")
			return false, err
		}
		if member.Status.State == raftv1beta2.RaftMemberNotReady {
			state = raftv1beta2.RaftPartitionNotReady
		}
	}

	if partition.Status.State != state {
		partition.Status.State = state
		if err := r.client.Status().Update(ctx, partition); err != nil {
			log.Error(err, "Reconcile RaftPartition")
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *RaftPartitionReconciler) reconcileDelete(ctx context.Context, partition *raftv1beta2.RaftPartition) (reconcile.Result, error) {
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
		return reconcile.Result{}, err
	}

	for _, member := range members.Items {
		if hasFinalizer(&member, raftPartitionKey) {
			removeFinalizer(&member, raftPartitionKey)
			if err := r.client.Update(ctx, &member); err != nil {
				log.Error(err, "Reconcile RaftPartition")
				return reconcile.Result{}, err
			}
		}
	}

	removeFinalizer(partition, raftPartitionKey)
	if err := r.client.Update(ctx, partition); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

var _ reconcile.Reconciler = (*RaftPartitionReconciler)(nil)
