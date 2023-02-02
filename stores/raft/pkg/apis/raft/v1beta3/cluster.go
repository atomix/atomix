// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta3

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

	// Logging is the store logging configuration
	Logging LoggingConfig `json:"logging"`

	// AverageRTT is the average round trip time between nodes
	AverageRTT metav1.Duration `json:"averageRTT"`

	// Persistence is the cluster persistence configuration
	Persistence RaftClusterPersistenceSpec `json:"persistence"`
}

type RaftClusterPersistenceSpec struct {
	Enabled      bool                                `json:"enabled"`
	StorageClass *string                             `json:"storageClass"`
	AccessModes  []corev1.PersistentVolumeAccessMode `json:"accessModes"`
	Size         *resource.Quantity                  `json:"size"`
	Selector     *metav1.LabelSelector               `json:"selector"`
}

// RaftClusterStatus defines the status of a RaftCluster
type RaftClusterStatus struct {
	State  RaftClusterState `json:"state"`
	Groups uint32           `json:"groups"`
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
