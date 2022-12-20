// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RaftStoreSpec specifies a RaftStore configuration
type RaftStoreSpec struct {
	RaftConfig        `json:",inline"`
	Cluster           corev1.ObjectReference `json:"cluster"`
	Partitions        uint32                 `json:"partitions"`
	ReplicationFactor *uint32                `json:"replicationFactor"`
}

// RaftStoreState is a state constant for RaftStore
type RaftStoreState string

const (
	// RaftStoreNotReady indicates a RaftStore is not yet ready
	RaftStoreNotReady RaftStoreState = "NotReady"
	// RaftStoreReady indicates a RaftStore is ready
	RaftStoreReady RaftStoreState = "Ready"
)

// RaftStoreStatus defines the status of a RaftStore
type RaftStoreStatus struct {
	ReplicationFactor *uint32        `json:"replicationFactor"`
	State             RaftStoreState `json:"state,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftStore is the Schema for the RaftStore API
// +k8s:openapi-gen=true
type RaftStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RaftStoreSpec   `json:"spec,omitempty"`
	Status            RaftStoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftStoreList contains a list of RaftStore
type RaftStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the RaftStore of items in the list
	Items []RaftStore `json:"items"`
}
