// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RaftClusterState is a state constant for RaftCluster
type RaftClusterState string

const (
	// RaftClusterNotReady indicates a RaftCluster is not yet ready
	RaftClusterNotReady RaftClusterState = "NotReady"
	// RaftClusterReady indicates a RaftCluster is ready
	RaftClusterReady RaftClusterState = "Ready"
)

// RaftClusterSpec specifies a RaftCluster configuration
type RaftClusterSpec struct {
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
	Config RaftClusterConfig `json:"config,omitempty"`
}

type RaftClusterConfig struct {
	// RTT is the estimated round trip time between nodes
	RTT metav1.Duration `json:"rtt"`

	// Server is the raft server configuration
	Server RaftServerConfig `json:"server,omitempty"`

	// Logging is the store logging configuration
	Logging LoggingConfig `json:"logging,omitempty"`
}

type RaftServerConfig struct {
	ReadBufferSize       *int               `json:"readBufferSize"`
	WriteBufferSize      *int               `json:"writeBufferSize"`
	MaxRecvMsgSize       *resource.Quantity `json:"maxRecvMsgSize"`
	MaxSendMsgSize       *resource.Quantity `json:"maxSendMsgSize"`
	NumStreamWorkers     *uint32            `json:"numStreamWorkers"`
	MaxConcurrentStreams *uint32            `json:"maxConcurrentStreams"`
}

// RaftClusterStatus defines the status of a RaftCluster
type RaftClusterStatus struct {
	State             RaftClusterState             `json:"state"`
	LastShardID       ShardID                      `json:"lastShardID"`
	PartitionStatuses []RaftClusterPartitionStatus `json:"partitionStatuses"`
}

type RaftClusterPartitionStatus struct {
	corev1.ObjectReference `json:",inline"`
	PartitionID            PartitionID `json:"partitionID"`
	ShardID                ShardID     `json:"shardID"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftCluster is the Schema for the RaftCluster API
// +k8s:openapi-gen=true
type RaftCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RaftClusterSpec   `json:"spec,omitempty"`
	Status            RaftClusterStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftClusterList contains a list of RaftCluster
type RaftClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the RaftCluster of items in the list
	Items []RaftCluster `json:"items"`
}
