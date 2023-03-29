// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"fmt"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
	raftv1beta3 "github.com/atomix/atomix/stores/raft/pkg/client/clientset/versioned/typed/raft/v1beta3"
	"github.com/onosproject/helmit/pkg/test"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"time"
)

type RaftStoreTestSuite struct {
	test.Suite
	*atomixv3beta4.AtomixV3beta4Client
	*raftv1beta3.RaftV1beta3Client
}

func (t *RaftStoreTestSuite) SetupSuite(ctx context.Context) {
	atomixV3beta4Client, err := atomixv3beta4.NewForConfig(t.Config())
	t.NoError(err)
	t.AtomixV3beta4Client = atomixV3beta4Client

	raftV1beta3Client, err := raftv1beta3.NewForConfig(t.Config())
	t.NoError(err)
	t.RaftV1beta3Client = raftV1beta3Client
}

func (t *RaftStoreTestSuite) TestRecreate(ctx context.Context) {
	t.T().Log("Create RaftCluster recreate")
	_, err := t.RaftClusters(t.Namespace()).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recreate",
		},
		Spec: v1beta3.RaftClusterSpec{
			ImagePullPolicy: corev1.PullNever,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster recreate ready")
	t.AwaitClusterReady(ctx, "recreate")

	pods, err := t.CoreV1().Pods(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=recreate",
	})
	t.NoError(err)
	if t.Len(pods.Items, 1) {
		t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)
	}

	_, err = t.RaftStores(t.Namespace()).Create(ctx, &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recreate",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "recreate",
			},
			Partitions: pointer.Int32(1),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftStore recreate ready")
	t.AwaitStoreReady(ctx, "recreate")

	err = t.RaftStores(t.Namespace()).Delete(ctx, "recreate", metav1.DeleteOptions{})
	t.NoError(err)

	err = t.RaftClusters(t.Namespace()).Delete(ctx, "recreate", metav1.DeleteOptions{})
	t.NoError(err)

	t.T().Log("Create RaftCluster recreate")
	_, err = t.RaftClusters(t.Namespace()).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recreate",
		},
		Spec: v1beta3.RaftClusterSpec{
			ImagePullPolicy: corev1.PullNever,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster recreate ready")
	t.AwaitClusterReady(ctx, "recreate")

	pods, err = t.CoreV1().Pods(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=recreate",
	})
	t.NoError(err)
	if t.Len(pods.Items, 1) {
		t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)
	}

	_, err = t.RaftStores(t.Namespace()).Create(ctx, &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recreate",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "recreate",
			},
			Partitions: pointer.Int32(1),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftStore recreate ready")
	t.AwaitStoreReady(ctx, "recreate")

	partitions, err := t.RaftPartitions(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=recreate",
	})
	t.NoError(err)
	if t.Len(partitions.Items, 1) {
		t.Equal(v1beta3.RaftPartitionReady, partitions.Items[0].Status.State)
	}

	replicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=recreate,raft.atomix.io/partition-id=%d", partitions.Items[0].Spec.PartitionID),
	})
	t.NoError(err)
	if t.Len(replicas.Items, 1) {
		t.Equal(v1beta3.RaftReplicaReady, replicas.Items[0].Status.State)
	}

	partitionPods := make(map[string]bool)
	for _, replica := range replicas.Items {
		partitionPods[replica.Spec.Pod.Name] = true
	}
	t.Len(partitionPods, 1)

	dataStore, err := t.DataStores(t.Namespace()).Get(ctx, "recreate", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(dataStore.Spec.Config.Raw)
}

func (t *RaftStoreTestSuite) TestSingleReplicaCluster(ctx context.Context) {
	t.T().Log("Create RaftCluster single-replica")
	_, err := t.RaftClusters(t.Namespace()).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "single-replica",
		},
		Spec: v1beta3.RaftClusterSpec{
			ImagePullPolicy: corev1.PullNever,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster single-replica ready")
	t.AwaitClusterReady(ctx, "single-replica")

	pods, err := t.CoreV1().Pods(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=single-replica",
	})
	t.NoError(err)
	t.Len(pods.Items, 1)
	t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)

	_, err = t.RaftStores(t.Namespace()).Create(ctx, &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "single-replica",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "single-replica",
			},
			Partitions: pointer.Int32(1),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftStore single-replica ready")
	t.AwaitStoreReady(ctx, "single-replica")

	partitions, err := t.RaftPartitions(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=single-replica",
	})
	t.NoError(err)
	t.Len(partitions.Items, 1)
	t.Equal(v1beta3.RaftPartitionReady, partitions.Items[0].Status.State)

	replicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=single-replica,raft.atomix.io/partition-id=%d", partitions.Items[0].Spec.PartitionID),
	})
	t.NoError(err)
	t.Len(replicas.Items, 1)
	t.Equal(v1beta3.RaftReplicaReady, replicas.Items[0].Status.State)

	partitionPods := make(map[string]bool)
	for _, replica := range replicas.Items {
		partitionPods[replica.Spec.Pod.Name] = true
	}
	t.Len(partitionPods, 1)

	dataStore, err := t.DataStores(t.Namespace()).Get(ctx, "single-replica", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(dataStore.Spec.Config.Raw)
}

func (t *RaftStoreTestSuite) TestMultiReplicaCluster(ctx context.Context) {
	t.T().Logf("Create RaftCluster multi-replica")
	_, err := t.RaftClusters(t.Namespace()).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "multi-replica",
		},
		Spec: v1beta3.RaftClusterSpec{
			Replicas:        pointer.Int32(3),
			ImagePullPolicy: corev1.PullNever,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster multi-replica ready")
	t.AwaitClusterReady(ctx, "multi-replica")

	pods, err := t.CoreV1().Pods(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=multi-replica",
	})
	t.NoError(err)
	t.Len(pods.Items, 3)
	t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[1].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[2].Status.ContainerStatuses[0].Ready)

	_, err = t.RaftStores(t.Namespace()).Create(ctx, &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "multi-replica",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "multi-replica",
			},
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftStore multi-replica ready")
	t.AwaitStoreReady(ctx, "multi-replica")

	storePartitions, err := t.RaftPartitions(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=multi-replica",
	})
	t.NoError(err)
	t.Len(storePartitions.Items, 1)
	t.Equal(v1beta3.RaftPartitionReady, storePartitions.Items[0].Status.State)

	storePartitionReplicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=multi-replica,raft.atomix.io/partition-id=%d", storePartitions.Items[0].Spec.PartitionID),
	})
	t.NoError(err)
	t.Len(storePartitionReplicas.Items, 3)
	t.Equal(v1beta3.RaftReplicaReady, storePartitionReplicas.Items[0].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, storePartitionReplicas.Items[1].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, storePartitionReplicas.Items[2].Status.State)

	partitionPods := make(map[string]bool)
	for _, replica := range storePartitionReplicas.Items {
		partitionPods[replica.Spec.Pod.Name] = true
	}
	t.Len(partitionPods, 3)

	dataStore, err := t.DataStores(t.Namespace()).Get(ctx, "multi-replica", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(dataStore.Spec.Config.Raw)
}

func (t *RaftStoreTestSuite) TestStoreReplicationFactor(ctx context.Context) {
	t.T().Logf("Create RaftCluster replication-factor")
	_, err := t.RaftClusters(t.Namespace()).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replication-factor",
		},
		Spec: v1beta3.RaftClusterSpec{
			Replicas:        pointer.Int32(5),
			ImagePullPolicy: corev1.PullNever,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Logf("Create RaftStore replication-factor-1")
	_, err = t.RaftStores(t.Namespace()).Create(ctx, &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replication-factor-1",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "replication-factor",
			},
			Partitions: pointer.Int32(1),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Logf("Create RaftStore replication-factor-2")
	_, err = t.RaftStores(t.Namespace()).Create(ctx, &v1beta3.RaftStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replication-factor-2",
		},
		Spec: v1beta3.RaftStoreSpec{
			Cluster: corev1.ObjectReference{
				Name: "replication-factor",
			},
			Partitions:        pointer.Int32(3),
			ReplicationFactor: pointer.Int32(3),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster replication-factor ready")
	t.AwaitClusterReady(ctx, "replication-factor")

	pods, err := t.CoreV1().Pods(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=replication-factor",
	})
	t.NoError(err)
	t.Len(pods.Items, 5)
	t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[1].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[2].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[3].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[4].Status.ContainerStatuses[0].Ready)

	t.T().Log("Await RaftCluster replication-factor-1 ready")
	t.AwaitStoreReady(ctx, "replication-factor-1")

	t.T().Log("Await RaftCluster replication-factor-2 ready")
	t.AwaitStoreReady(ctx, "replication-factor-2")

	store1Partitions, err := t.RaftPartitions(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=replication-factor-1",
	})
	t.NoError(err)
	t.Len(store1Partitions.Items, 1)
	t.Equal(v1beta3.RaftPartitionReady, store1Partitions.Items[0].Status.State)

	store1Partition1Replicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=replication-factor-1,raft.atomix.io/partition-id=%d", store1Partitions.Items[0].Spec.PartitionID),
	})
	t.NoError(err)
	t.Len(store1Partition1Replicas.Items, 5)
	t.Equal(v1beta3.RaftReplicaReady, store1Partition1Replicas.Items[0].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store1Partition1Replicas.Items[1].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store1Partition1Replicas.Items[2].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store1Partition1Replicas.Items[3].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store1Partition1Replicas.Items[4].Status.State)

	store1Partition1Pods := make(map[string]bool)
	for _, replica := range store1Partition1Replicas.Items {
		store1Partition1Pods[replica.Spec.Pod.Name] = true
	}
	t.Len(store1Partition1Pods, 5)

	store1DataStore, err := t.DataStores(t.Namespace()).Get(ctx, "replication-factor-1", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(store1DataStore.Spec.Config.Raw)

	store2Partitions, err := t.RaftPartitions(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=replication-factor-2",
	})
	t.NoError(err)
	t.Len(store2Partitions.Items, 3)
	t.Equal(v1beta3.RaftPartitionReady, store2Partitions.Items[0].Status.State)
	t.Equal(v1beta3.RaftPartitionReady, store2Partitions.Items[1].Status.State)
	t.Equal(v1beta3.RaftPartitionReady, store2Partitions.Items[2].Status.State)

	store2Partition1Replicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=replication-factor-2,raft.atomix.io/partition-id=%d", store2Partitions.Items[0].Spec.PartitionID),
	})
	t.NoError(err)
	t.Len(store2Partition1Replicas.Items, 3)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition1Replicas.Items[0].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition1Replicas.Items[1].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition1Replicas.Items[2].Status.State)

	store2Partition1Pods := make(map[string]bool)
	for _, replica := range store2Partition1Replicas.Items {
		store2Partition1Pods[replica.Spec.Pod.Name] = true
	}
	t.Len(store2Partition1Pods, 3)

	store2Partition2Replicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=replication-factor-2,raft.atomix.io/partition-id=%d", store2Partitions.Items[1].Spec.PartitionID),
	})
	t.NoError(err)
	t.Len(store2Partition2Replicas.Items, 3)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition2Replicas.Items[0].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition2Replicas.Items[1].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition2Replicas.Items[2].Status.State)

	store2Partition2Pods := make(map[string]bool)
	for _, replica := range store2Partition2Replicas.Items {
		store2Partition2Pods[replica.Spec.Pod.Name] = true
	}
	t.Len(store2Partition2Pods, 3)

	store2Partition3Replicas, err := t.RaftReplicas(t.Namespace()).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("raft.atomix.io/store=replication-factor-2,raft.atomix.io/partition-id=%d", store2Partitions.Items[2].Spec.PartitionID),
	})
	t.NoError(err)
	t.Len(store2Partition3Replicas.Items, 3)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition3Replicas.Items[0].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition3Replicas.Items[1].Status.State)
	t.Equal(v1beta3.RaftReplicaReady, store2Partition3Replicas.Items[2].Status.State)

	store2Partition3Pods := make(map[string]bool)
	for _, replica := range store2Partition3Replicas.Items {
		store2Partition3Pods[replica.Spec.Pod.Name] = true
	}
	t.Len(store2Partition3Pods, 3)

	store2DataStore, err := t.DataStores(t.Namespace()).Get(ctx, "replication-factor-2", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(store2DataStore.Spec.Config.Raw)
}

func (t *RaftStoreTestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (t *RaftStoreTestSuite) AwaitClusterReady(ctx context.Context, name string) {
	t.Await(func() bool {
		cluster, err := t.RaftClusters(t.Namespace()).Get(ctx, name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta3.RaftClusterReady
	})
}

func (t *RaftStoreTestSuite) AwaitStoreReady(ctx context.Context, name string) {
	t.Await(func() bool {
		cluster, err := t.RaftStores(t.Namespace()).Get(ctx, name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta3.RaftStoreReady
	})
}

func (t *RaftStoreTestSuite) NoError(err error, args ...interface{}) bool {
	if err == context.DeadlineExceeded {
		t.Fail(err.Error())
	}
	return t.Suite.NoError(err, args...)
}
