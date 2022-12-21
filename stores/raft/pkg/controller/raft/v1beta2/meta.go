// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	"fmt"
	raftv1beta2 "github.com/atomix/atomix/stores/raft/pkg/apis/raft/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
)

// getResourceName returns the given resource name for the given object name
func getResourceName(name string, resource string) string {
	return fmt.Sprintf("%s-%s", name, resource)
}

// getHeadlessServiceName returns the headless service name for the given cluster
func getHeadlessServiceName(cluster *raftv1beta2.RaftCluster) string {
	return getResourceName(cluster.Name, headlessServiceSuffix)
}

// getClusterDomain returns Kubernetes cluster domain, default to "cluster.local"
func getClusterDomain() string {
	clusterDomain := os.Getenv(clusterDomainEnv)
	if clusterDomain == "" {
		apiSvc := "kubernetes.default.svc"
		cname, err := net.LookupCNAME(apiSvc)
		if err != nil {
			return "cluster.local"
		}
		clusterDomain = strings.TrimSuffix(strings.TrimPrefix(cname, apiSvc+"."), ".")
	}
	return clusterDomain
}

func getClusterNamespace(object metav1.Object, clusterRef corev1.ObjectReference) string {
	if clusterRef.Namespace != "" {
		return clusterRef.Namespace
	}
	return object.GetNamespace()
}

func getDNSName(namespace string, cluster string, name string) string {
	return fmt.Sprintf("%s.%s-%s.%s.svc.%s", name, cluster, headlessServiceSuffix, namespace, getClusterDomain())
}

// getClusterPodDNSName returns the fully qualified DNS name for the given pod ID
func getClusterPodDNSName(cluster *raftv1beta2.RaftCluster, name string) string {
	return fmt.Sprintf("%s.%s.%s.svc.%s", name, getHeadlessServiceName(cluster), cluster.Namespace, getClusterDomain())
}

func getMemberPodOrdinal(cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID) int {
	return (int(partition.Spec.Replicas)*int(partition.Spec.ShardID) + (int(memberID) - 1)) % int(cluster.Spec.Replicas)
}

func getMemberPodName(cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID) string {
	return fmt.Sprintf("%s-%d", cluster.Name, getMemberPodOrdinal(cluster, partition, memberID))
}

// newClusterLabels returns the labels for the given cluster
func newClusterLabels(cluster *raftv1beta2.RaftCluster) map[string]string {
	labels := make(map[string]string)
	for key, value := range cluster.Labels {
		labels[key] = value
	}
	labels[raftClusterKey] = cluster.Name
	return labels
}

func newClusterSelector(cluster *raftv1beta2.RaftCluster) map[string]string {
	return map[string]string{
		raftClusterKey: cluster.Name,
	}
}

// newPartitionLabels returns the labels for the given partition
func newPartitionLabels(cluster *raftv1beta2.RaftCluster, store metav1.Object, partitionID raftv1beta2.PartitionID, shardID raftv1beta2.ShardID) map[string]string {
	labels := make(map[string]string)
	for key, value := range store.GetLabels() {
		labels[key] = value
	}
	labels[storeKey] = store.GetName()
	labels[raftStoreKey] = store.GetName()
	labels[raftClusterKey] = cluster.Name
	labels[raftPartitionKey] = strconv.Itoa(int(partitionID))
	labels[raftShardKey] = strconv.Itoa(int(shardID))
	return labels
}

func newPartitionAnnotations(cluster *raftv1beta2.RaftCluster, store metav1.Object, partitionID raftv1beta2.PartitionID, shardID raftv1beta2.ShardID) map[string]string {
	annotations := make(map[string]string)
	for key, value := range store.GetLabels() {
		annotations[key] = value
	}
	annotations[storeKey] = store.GetName()
	annotations[raftStoreKey] = store.GetName()
	annotations[raftClusterKey] = cluster.Name
	annotations[raftPartitionKey] = strconv.Itoa(int(partitionID))
	annotations[raftShardKey] = strconv.Itoa(int(shardID))
	return annotations
}

func newPartitionSelector(partition *raftv1beta2.RaftPartition) map[string]string {
	return map[string]string{
		raftClusterKey:   partition.Spec.Cluster.Name,
		raftPartitionKey: strconv.Itoa(int(partition.Spec.PartitionID)),
		raftShardKey:     strconv.Itoa(int(partition.Spec.ShardID)),
	}
}

// newMemberLabels returns the labels for the given cluster
func newMemberLabels(cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID, raftNodeID raftv1beta2.ReplicaID) map[string]string {
	labels := make(map[string]string)
	for key, value := range partition.Labels {
		labels[key] = value
	}
	labels[podKey] = getMemberPodName(cluster, partition, memberID)
	labels[raftMemberKey] = strconv.Itoa(int(memberID))
	labels[raftReplicaKey] = strconv.Itoa(int(raftNodeID))
	return labels
}

func newClusterAnnotations(cluster *raftv1beta2.RaftCluster) map[string]string {
	annotations := make(map[string]string)
	for key, value := range cluster.Annotations {
		annotations[key] = value
	}
	annotations[raftClusterKey] = cluster.Name
	return annotations
}

func newMemberAnnotations(cluster *raftv1beta2.RaftCluster, partition *raftv1beta2.RaftPartition, memberID raftv1beta2.MemberID, raftNodeID raftv1beta2.ReplicaID) map[string]string {
	annotations := make(map[string]string)
	for key, value := range partition.Labels {
		annotations[key] = value
	}
	annotations[podKey] = getMemberPodName(cluster, partition, memberID)
	annotations[raftMemberKey] = strconv.Itoa(int(memberID))
	annotations[raftReplicaKey] = strconv.Itoa(int(raftNodeID))
	return annotations
}

func getImage(cluster *raftv1beta2.RaftCluster) string {
	if cluster.Spec.Image != "" {
		return cluster.Spec.Image
	}
	return getDefaultImage()
}

func getDefaultImage() string {
	image := os.Getenv(defaultImageEnv)
	if image == "" {
		image = defaultImage
	}
	return image
}

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
