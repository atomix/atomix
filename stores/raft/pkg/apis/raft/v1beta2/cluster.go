// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MultiRaftClusterState is a state constant for MultiRaftCluster
type MultiRaftClusterState string

const (
	// MultiRaftClusterNotReady indicates a MultiRaftCluster is not yet ready
	MultiRaftClusterNotReady MultiRaftClusterState = "NotReady"
	// MultiRaftClusterReady indicates a MultiRaftCluster is ready
	MultiRaftClusterReady MultiRaftClusterState = "Ready"
)

// MultiRaftClusterSpec specifies a MultiRaftCluster configuration
type MultiRaftClusterSpec struct {
	// Replicas is the number of raft replicas
	Replicas uint32 `json:"replicas,omitempty"`

	// Image is the image to run
	Image string `json:"image,omitempty"`

	// ImagePullPolicy is the pull policy to apply
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is a list of secrets for pulling images
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// SecurityContext is a pod security context
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// VolumeClaimTemplate is the volume claim template for Raft logs
	VolumeClaimTemplate *corev1.PersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`

	// Config is the raft cluster configuration
	Config MultiRaftClusterConfig `json:"config,omitempty"`
}

type MultiRaftClusterConfig struct {
	// RTT is the estimated round trip time between nodes
	RTT metav1.Duration `json:"rtt"`

	// Server is the raft server configuration
	Server MultiRaftServerConfig `json:"server,omitempty"`

	// Logging is the store logging configuration
	Logging LoggingConfig `json:"logging,omitempty"`
}

type MultiRaftServerConfig struct {
	ReadBufferSize       *int               `json:"readBufferSize"`
	WriteBufferSize      *int               `json:"writeBufferSize"`
	MaxRecvMsgSize       *resource.Quantity `json:"maxRecvMsgSize"`
	MaxSendMsgSize       *resource.Quantity `json:"maxSendMsgSize"`
	NumStreamWorkers     *uint32            `json:"numStreamWorkers"`
	MaxConcurrentStreams *uint32            `json:"maxConcurrentStreams"`
}

// MultiRaftClusterStatus defines the status of a MultiRaftCluster
type MultiRaftClusterStatus struct {
	State             MultiRaftClusterState             `json:"state"`
	LastShardID       ShardID                           `json:"lastShardID"`
	PartitionStatuses []MultiRaftClusterPartitionStatus `json:"partitionStatuses"`
}

type MultiRaftClusterPartitionStatus struct {
	corev1.LocalObjectReference `json:",inline"`
	PartitionID                 PartitionID `json:"partitionID"`
	ShardID                     ShardID     `json:"shardID"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MultiRaftCluster is the Schema for the MultiRaftCluster API
// +k8s:openapi-gen=true
type MultiRaftCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MultiRaftClusterSpec   `json:"spec,omitempty"`
	Status            MultiRaftClusterStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MultiRaftClusterList contains a list of MultiRaftCluster
type MultiRaftClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the MultiRaftCluster of items in the list
	Items []MultiRaftCluster `json:"items"`
}
