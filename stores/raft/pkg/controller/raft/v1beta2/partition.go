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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"
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
	return r.reconcilePartition(ctx, log, partition)
}

func (r *RaftPartitionReconciler) reconcilePartition(ctx context.Context, log logging.Logger, partition *raftv1beta2.RaftPartition) (reconcile.Result, error) {
	log = log.WithFields(
		logging.Uint64("PartitionID", uint64(partition.Spec.PartitionID)),
		logging.Uint64("ShardID", uint64(partition.Spec.ShardID)))

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
	return reconcile.Result{}, nil
}

func (r *RaftPartitionReconciler) reconcileMembers(ctx context.Context, log logging.Logger, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition) (bool, error) {
	allReady := true
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		memberID := raftv1beta2.MemberID(ordinal)
		if status, ok, err := r.reconcileMember(ctx, log, cluster, partition, memberID); err != nil {
			return false, err
		} else if ok {
			return true, nil
		} else if status != raftv1beta2.RaftMemberReady {
			allReady = false
		}
	}

	switch partition.Status.State {
	case raftv1beta2.RaftPartitionInitializing:
		partition.Status.State = raftv1beta2.RaftPartitionRunning
		partition.Status.Replicas = partition.Spec.Replicas
	case raftv1beta2.RaftPartitionReconfiguring, raftv1beta2.RaftPartitionRunning:
		if !allReady {
			return false, nil
		}
		partition.Status.State = raftv1beta2.RaftPartitionReady
	case raftv1beta2.RaftPartitionReady:
		if allReady {
			return false, nil
		}
		partition.Status.State = raftv1beta2.RaftPartitionRunning
	}

	log.Infow("Partition status changed", logging.String("Status", string(partition.Status.State)))
	if err := r.client.Status().Update(ctx, partition); err != nil {
		return false, logError(log, err)
	}
	return true, nil
}

func (r *RaftPartitionReconciler) reconcileMember(ctx context.Context, log logging.Logger, cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID) (raftv1beta2.RaftMemberState, bool, error) {
	log = log.WithFields(logging.Uint64("MemberID", uint64(memberID)))
	memberName := types.NamespacedName{
		Namespace: partition.Namespace,
		Name:      fmt.Sprintf("%s-%d", partition.Name, memberID),
	}
	member := &raftv1beta2.RaftMember{}
	if err := r.client.Get(ctx, memberName, member); err != nil {
		if !k8serrors.IsNotFound(err) {
			log.Error(err)
			return "", false, err
		}

		podName := types.NamespacedName{
			Namespace: getClusterNamespace(partition, partition.Spec.Cluster),
			Name:      getMemberPodName(cluster, partition, memberID),
		}
		pod := &corev1.Pod{}
		if err := r.client.Get(ctx, podName, pod); err != nil {
			if !k8serrors.IsNotFound(err) {
				log.Error(err)
				return "", false, err
			}
			log.Debugw("Member pod not found", logging.Stringer("Pod", podName))
			return "", false, nil
		}

		member = &raftv1beta2.RaftMember{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: memberName.Namespace,
				Name:      memberName.Name,
			},
			Spec: raftv1beta2.RaftMemberSpec{
				Cluster:  partition.Spec.Cluster,
				ShardID:  partition.Spec.ShardID,
				MemberID: memberID,
				Pod: corev1.LocalObjectReference{
					Name: pod.Name,
				},
				Type:   raftv1beta2.RaftVoter,
				Peers:  partition.Spec.Replicas,
				Config: partition.Spec.RaftConfig,
			},
		}

		switch partition.Status.State {
		case raftv1beta2.RaftPartitionInitializing:
			member.Spec.ReplicaID = raftv1beta2.ReplicaID(memberID)
		default:
			log.Debug("Allocating new replica")
			partition.Status.State = raftv1beta2.RaftPartitionReconfiguring
			partition.Status.Replicas++
			if err := r.client.Status().Update(ctx, partition); err != nil {
				return "", false, logError(log, err)
			}
			member.Spec.ReplicaID = raftv1beta2.ReplicaID(partition.Status.Replicas)
			member.Spec.Join = true
		}

		member.Labels = newMemberLabels(cluster, partition, memberID, member.Spec.ReplicaID)
		member.Annotations = newMemberAnnotations(cluster, partition, memberID, member.Spec.ReplicaID)

		log.Infow("Creating RaftMember",
			logging.Uint64("ReplicaID", uint64(member.Spec.ReplicaID)))
		if err := controllerutil.SetControllerReference(partition, member, r.scheme); err != nil {
			log.Error(err)
			return "", false, err
		}
		if err := controllerutil.SetOwnerReference(pod, member, r.scheme); err != nil {
			log.Error(err)
			return "", false, err
		}
		if err := r.client.Create(ctx, member); err != nil {
			return "", false, logError(log, err)
		}
		return member.Status.State, true, nil
	}
	return member.Status.State, false, nil
}

var _ reconcile.Reconciler = (*RaftPartitionReconciler)(nil)
