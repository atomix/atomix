// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	atomixv3beta3 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta3"
	atomixv3beta4 "github.com/atomix/atomix/controller/pkg/client/clientset/versioned/typed/atomix/v3beta4"
	"github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta3"
	clientv1beta2 "github.com/atomix/atomix/stores/raft/pkg/client/clientset/versioned/typed/raft/v1beta2"
	clientv1beta3 "github.com/atomix/atomix/stores/raft/pkg/client/clientset/versioned/typed/raft/v1beta3"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/pointer"
	"os"
	"testing"
	"time"
)

func main() {
	cmd := getCommand()
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "atomix-raft-controller-test",
		RunE: func(cmd *cobra.Command, args []string) error {
			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				return err
			}

			config, err := rest.InClusterConfig()
			if err != nil {
				return err
			}

			k8sClient, err := kubernetes.NewForConfig(config)
			if err != nil {
				return err
			}

			atomixV3beta3Client, err := atomixv3beta3.NewForConfig(config)
			if err != nil {
				return err
			}

			atomixV3beta4Client, err := atomixv3beta4.NewForConfig(config)
			if err != nil {
				return err
			}

			raftV1beta2Client, err := clientv1beta2.NewForConfig(config)
			if err != nil {
				return err
			}

			raftV1beta3Client, err := clientv1beta3.NewForConfig(config)
			if err != nil {
				return err
			}

			tests := []testing.InternalTest{
				{
					Name: "TestController",
					F: func(t *testing.T) {
						suite.Run(t, &TestSuite{
							name:                os.Getenv("TEST_NAME"),
							timeout:             timeout,
							Clientset:           k8sClient,
							RaftV1beta2Client:   raftV1beta2Client,
							RaftV1beta3Client:   raftV1beta3Client,
							AtomixV3beta3Client: atomixV3beta3Client,
							AtomixV3beta4Client: atomixV3beta4Client,
						})
					},
				},
			}

			// Hack to enable verbose testing.
			os.Args = []string{
				os.Args[0],
				"-test.v",
			}

			testing.Main(func(_, _ string) (bool, error) { return true, nil }, tests, nil, nil)
			return nil
		},
	}

	cmd.Flags().DurationP("timeout", "t", 10*time.Minute, "the test timeout")
	return cmd
}

type TestSuite struct {
	suite.Suite
	*kubernetes.Clientset
	*clientv1beta2.RaftV1beta2Client
	*clientv1beta3.RaftV1beta3Client
	*atomixv3beta3.AtomixV3beta3Client
	*atomixv3beta4.AtomixV3beta4Client
	name    string
	timeout time.Duration
}

func (t *TestSuite) AtomixV3beta3() *atomixv3beta3.AtomixV3beta3Client {
	return t.AtomixV3beta3Client
}

func (t *TestSuite) AtomixV3beta4() *atomixv3beta4.AtomixV3beta4Client {
	return t.AtomixV3beta4Client
}

func (t *TestSuite) RaftV1beta2() *clientv1beta2.RaftV1beta2Client {
	return t.RaftV1beta2Client
}

func (t *TestSuite) RaftV1beta3() *clientv1beta3.RaftV1beta3Client {
	return t.RaftV1beta3Client
}

func (t *TestSuite) TestSingleReplicaCluster() {
	namespace := t.setup("single-replica")
	defer t.teardown("single-replica")

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	t.T().Log("Create RaftCluster single-replica")
	_, err := t.RaftV1beta3().RaftClusters(namespace).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "single-replica",
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster single-replica ready")
	t.AwaitClusterReady(ctx, namespace, "single-replica")

	pods, err := t.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=single-replica",
	})
	t.NoError(err)
	t.Len(pods.Items, 1)
	t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)

	_, err = t.RaftV1beta3().RaftStores(namespace).Create(ctx, &v1beta3.RaftStore{
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
	t.AwaitStoreReady(ctx, namespace, "single-replica")

	partitions, err := t.RaftV1beta3().RaftPartitions(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=single-replica",
	})
	t.NoError(err)
	t.Len(partitions.Items, 1)
	t.Equal(v1beta3.RaftPartitionReady, partitions.Items[0].Status.State)

	replicas, err := t.RaftV1beta3().RaftReplicas(namespace).List(ctx, metav1.ListOptions{
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

	dataStore, err := t.AtomixV3beta4().DataStores(namespace).Get(ctx, "single-replica", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(dataStore.Spec.Config.Raw)
}

func (t *TestSuite) TestMultiReplicaCluster() {
	namespace := t.setup("multi-replica")
	defer t.teardown("multi-replica")

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	t.T().Logf("Create RaftCluster multi-replica")
	_, err := t.RaftV1beta3().RaftClusters(namespace).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "multi-replica",
		},
		Spec: v1beta3.RaftClusterSpec{
			Replicas: pointer.Int32(3),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Log("Await RaftCluster multi-replica ready")
	t.AwaitClusterReady(ctx, namespace, "multi-replica")

	pods, err := t.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/cluster=multi-replica",
	})
	t.NoError(err)
	t.Len(pods.Items, 3)
	t.True(pods.Items[0].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[1].Status.ContainerStatuses[0].Ready)
	t.True(pods.Items[2].Status.ContainerStatuses[0].Ready)

	_, err = t.RaftV1beta3().RaftStores(namespace).Create(ctx, &v1beta3.RaftStore{
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
	t.AwaitStoreReady(ctx, namespace, "multi-replica")

	storePartitions, err := t.RaftV1beta3().RaftPartitions(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=multi-replica",
	})
	t.NoError(err)
	t.Len(storePartitions.Items, 1)
	t.Equal(v1beta3.RaftPartitionReady, storePartitions.Items[0].Status.State)

	storePartitionReplicas, err := t.RaftV1beta3().RaftReplicas(namespace).List(ctx, metav1.ListOptions{
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

	dataStore, err := t.AtomixV3beta4().DataStores(namespace).Get(ctx, "multi-replica", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(dataStore.Spec.Config.Raw)
}

func (t *TestSuite) TestStoreReplicationFactor() {
	namespace := t.setup("multi-replica")
	defer t.teardown("multi-replica")

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	t.T().Logf("Create RaftCluster replication-factor")
	_, err := t.RaftV1beta3().RaftClusters(namespace).Create(ctx, &v1beta3.RaftCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replication-factor",
		},
		Spec: v1beta3.RaftClusterSpec{
			Replicas: pointer.Int32(5),
		},
	}, metav1.CreateOptions{})
	t.NoError(err)

	t.T().Logf("Create RaftStore replication-factor-1")
	_, err = t.RaftV1beta3().RaftStores(namespace).Create(ctx, &v1beta3.RaftStore{
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
	_, err = t.RaftV1beta3().RaftStores(namespace).Create(ctx, &v1beta3.RaftStore{
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
	t.AwaitClusterReady(ctx, namespace, "replication-factor")

	pods, err := t.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
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
	t.AwaitStoreReady(ctx, namespace, "replication-factor-1")

	t.T().Log("Await RaftCluster replication-factor-2 ready")
	t.AwaitStoreReady(ctx, namespace, "replication-factor-2")

	store1Partitions, err := t.RaftV1beta3().RaftPartitions(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=replication-factor-1",
	})
	t.NoError(err)
	t.Len(store1Partitions.Items, 1)
	t.Equal(v1beta3.RaftPartitionReady, store1Partitions.Items[0].Status.State)

	store1Partition1Replicas, err := t.RaftV1beta3().RaftReplicas(namespace).List(ctx, metav1.ListOptions{
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

	store1DataStore, err := t.AtomixV3beta4().DataStores(namespace).Get(ctx, "replication-factor-1", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(store1DataStore.Spec.Config.Raw)

	store2Partitions, err := t.RaftV1beta3().RaftPartitions(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "raft.atomix.io/store=replication-factor-2",
	})
	t.NoError(err)
	t.Len(store2Partitions.Items, 3)
	t.Equal(v1beta3.RaftPartitionReady, store2Partitions.Items[0].Status.State)
	t.Equal(v1beta3.RaftPartitionReady, store2Partitions.Items[1].Status.State)
	t.Equal(v1beta3.RaftPartitionReady, store2Partitions.Items[2].Status.State)

	store2Partition1Replicas, err := t.RaftV1beta3().RaftReplicas(namespace).List(ctx, metav1.ListOptions{
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

	store2Partition2Replicas, err := t.RaftV1beta3().RaftReplicas(namespace).List(ctx, metav1.ListOptions{
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

	store2Partition3Replicas, err := t.RaftV1beta3().RaftReplicas(namespace).List(ctx, metav1.ListOptions{
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

	store2DataStore, err := t.AtomixV3beta4().DataStores(namespace).Get(ctx, "replication-factor-2", metav1.GetOptions{})
	t.NoError(err)
	t.NotEmpty(store2DataStore.Spec.Config.Raw)
}

func (t *TestSuite) Await(predicate func() bool) {
	for {
		if predicate() {
			return
		}
		time.Sleep(time.Second)
	}
}

func (t *TestSuite) AwaitClusterReady(ctx context.Context, namespace, name string) {
	t.Await(func() bool {
		cluster, err := t.RaftV1beta3().RaftClusters(namespace).Get(ctx, name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta3.RaftClusterReady
	})
}

func (t *TestSuite) AwaitStoreReady(ctx context.Context, namespace, name string) {
	t.Await(func() bool {
		cluster, err := t.RaftV1beta3().RaftStores(namespace).Get(ctx, name, metav1.GetOptions{})
		t.NoError(err)
		return cluster.Status.State == v1beta3.RaftStoreReady
	})
}

func (t *TestSuite) NoError(err error, args ...interface{}) bool {
	if err == context.DeadlineExceeded {
		t.Fail(err.Error())
	}
	return t.Suite.NoError(err, args...)
}

func (t *TestSuite) setup(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	namespace := fmt.Sprintf("%s-%s", t.name, name)
	_, err := t.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}, metav1.CreateOptions{})
	t.NoError(err)
	return namespace
}

func (t *TestSuite) teardown(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	namespace := fmt.Sprintf("%s-%s", t.name, name)
	err := t.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	t.NoError(err)
}
