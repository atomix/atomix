// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta3

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
	"time"

	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
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
	err = controller.Watch(&source.Kind{Type: &raftv1beta3.RaftPartition{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftReplica
	err = controller.Watch(&source.Kind{Type: &raftv1beta3.RaftReplica{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &raftv1beta3.RaftPartition{},
		IsController: true,
	})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource RaftCluster
	err = controller.Watch(&source.Kind{Type: &raftv1beta3.RaftCluster{}}, handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		options := &client.ListOptions{
			LabelSelector: labels.SelectorFromSet(map[string]string{
				raftNamespaceKey: object.GetNamespace(),
				raftClusterKey:   object.GetName(),
			}),
		}
		partitions := &raftv1beta3.RaftPartitionList{}
		if err := mgr.GetClient().List(context.Background(), partitions, options); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, partition := range partitions.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: partition.Namespace,
					Name:      partition.Name,
				},
			})
		}
		return requests
	}))
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
		partitions := &raftv1beta3.RaftPartitionList{}
		if err := mgr.GetClient().List(context.Background(), partitions, options); err != nil {
			log.Error(err)
			return nil
		}

		var requests []reconcile.Request
		for _, partition := range partitions.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: partition.Namespace,
					Name:      partition.Name,
				},
			})
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

	partition := &raftv1beta3.RaftPartition{}
	if ok, err := get(r.client, ctx, request.NamespacedName, partition, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}
	return r.reconcilePartition(ctx, log, partition)
}

func (r *RaftPartitionReconciler) reconcilePartition(ctx context.Context, log logging.Logger, partition *raftv1beta3.RaftPartition) (reconcile.Result, error) {
	log = log.WithFields(
		logging.Uint64("PartitionID", uint64(partition.Spec.PartitionID)),
		logging.Uint64("GroupID", uint64(partition.Spec.GroupID)))

	cluster := &raftv1beta3.RaftCluster{}
	clusterName := types.NamespacedName{
		Namespace: partition.Annotations[raftNamespaceKey],
		Name:      partition.Annotations[raftClusterKey],
	}
	if ok, err := get(r.client, ctx, clusterName, cluster, log); err != nil {
		return reconcile.Result{}, err
	} else if !ok {
		return reconcile.Result{}, nil
	}

	if ok, err := r.reconcileReplicas(ctx, log, cluster, partition); err != nil {
		return reconcile.Result{}, err
	} else if ok {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, nil
}

func (r *RaftPartitionReconciler) reconcileReplicas(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster, partition *raftv1beta3.RaftPartition) (bool, error) {
	allReady := true
	for ordinal := 1; ordinal <= int(partition.Spec.Replicas); ordinal++ {
		replicaID := raftv1beta3.ReplicaID(ordinal)
		if status, ok, err := r.reconcileReplica(ctx, log, cluster, partition, replicaID); err != nil {
			return false, err
		} else if ok {
			return true, nil
		} else if status != raftv1beta3.RaftReplicaReady {
			allReady = false
		}
	}

	switch partition.Status.State {
	case raftv1beta3.RaftPartitionInitializing:
		partition.Status.State = raftv1beta3.RaftPartitionRunning
		partition.Status.Members = partition.Spec.Replicas
	case raftv1beta3.RaftPartitionReconfiguring, raftv1beta3.RaftPartitionRunning:
		if !allReady {
			return false, nil
		}
		partition.Status.State = raftv1beta3.RaftPartitionReady
	case raftv1beta3.RaftPartitionReady:
		if allReady {
			return false, nil
		}
		partition.Status.State = raftv1beta3.RaftPartitionRunning
	}

	log.Infow("Partition status changed", logging.String("Status", string(partition.Status.State)))
	if err := updateStatus(r.client, ctx, partition, log); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RaftPartitionReconciler) reconcileReplica(ctx context.Context, log logging.Logger, cluster *raftv1beta3.RaftCluster, partition *raftv1beta3.RaftPartition, replicaID raftv1beta3.ReplicaID) (raftv1beta3.RaftReplicaState, bool, error) {
	log = log.WithFields(logging.Uint64("ReplicaID", uint64(replicaID)))
	replicaName := types.NamespacedName{
		Namespace: partition.Namespace,
		Name:      fmt.Sprintf("%s-%d", partition.Name, replicaID),
	}
	replica := &raftv1beta3.RaftReplica{}
	if ok, err := get(r.client, ctx, replicaName, replica, log); err != nil {
		return "", false, err
	} else if !ok {
		podName := types.NamespacedName{
			Namespace: cluster.Namespace,
			Name:      getReplicaPodName(cluster, partition, replicaID),
		}
		pod := &corev1.Pod{}
		if ok, err := get(r.client, ctx, podName, pod, log); err != nil {
			return "", false, err
		} else if !ok {
			return "", true, nil
		}

		replica = &raftv1beta3.RaftReplica{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: replicaName.Namespace,
				Name:      replicaName.Name,
			},
			Spec: raftv1beta3.RaftReplicaSpec{
				GroupID:   partition.Spec.GroupID,
				ReplicaID: replicaID,
				Pod: corev1.LocalObjectReference{
					Name: pod.Name,
				},
				Type:  raftv1beta3.RaftVoter,
				Peers: partition.Spec.Replicas,
			},
		}

		switch partition.Status.State {
		case raftv1beta3.RaftPartitionInitializing:
			replica.Spec.MemberID = raftv1beta3.MemberID(replicaID)
		default:
			partition.Status.State = raftv1beta3.RaftPartitionReconfiguring
			partition.Status.Members++
			log.Debugw("Allocating new member", logging.Uint64("MemberID", uint64(partition.Status.Members)))
			if err := updateStatus(r.client, ctx, partition, log); err != nil {
				return "", false, err
			}
			replica.Spec.MemberID = raftv1beta3.MemberID(partition.Status.Members)
			replica.Spec.Join = true
		}

		replica.Labels = newReplicaLabels(cluster, partition, replicaID, replica.Spec.MemberID)
		replica.Annotations = newReplicaAnnotations(cluster, partition, replicaID, replica.Spec.MemberID)

		log.Infow("Creating RaftReplica",
			logging.Uint64("MemberID", uint64(replica.Spec.MemberID)))
		if err := controllerutil.SetControllerReference(partition, replica, r.scheme); err != nil {
			log.Error(err)
			return "", false, err
		}
		if err := controllerutil.SetOwnerReference(pod, replica, r.scheme); err != nil {
			log.Error(err)
			return "", false, err
		}
		if err := create(r.client, ctx, replica, log); err != nil {
			return "", false, err
		}
		return replica.Status.State, true, nil
	}
	return replica.Status.State, false, nil
}

var _ reconcile.Reconciler = (*RaftPartitionReconciler)(nil)
