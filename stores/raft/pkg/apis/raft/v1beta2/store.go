// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MultiRaftStoreSpec specifies a MultiRaftStore configuration
type MultiRaftStoreSpec struct {
	RaftConfig        `json:",inline"`
	Cluster           corev1.LocalObjectReference `json:"cluster"`
	Partitions        uint32                      `json:"partitions"`
	ReplicationFactor *uint32                     `json:"replicationFactor"`
}

// MultiRaftStoreState is a state constant for MultiRaftStore
type MultiRaftStoreState string

const (
	// MultiRaftStoreNotReady indicates a MultiRaftStore is not yet ready
	MultiRaftStoreNotReady MultiRaftStoreState = "NotReady"
	// MultiRaftStoreReady indicates a MultiRaftStore is ready
	MultiRaftStoreReady MultiRaftStoreState = "Ready"
)

// MultiRaftStoreStatus defines the status of a MultiRaftStore
type MultiRaftStoreStatus struct {
	ReplicationFactor *uint32             `json:"replicationFactor"`
	State             MultiRaftStoreState `json:"state,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MultiRaftStore is the Schema for the MultiRaftStore API
// +k8s:openapi-gen=true
type MultiRaftStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MultiRaftStoreSpec   `json:"spec,omitempty"`
	Status            MultiRaftStoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MultiRaftStoreList contains a list of MultiRaftStore
type MultiRaftStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the MultiRaftStore of items in the list
	Items []MultiRaftStore `json:"items"`
}
