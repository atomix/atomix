// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConsensusStoreSpec specifies a ConsensusStore configuration
type ConsensusStoreSpec struct {
	MultiRaftClusterSpec `json:",inline"`
}

// ConsensusStoreState is a state constant for ConsensusStore
type ConsensusStoreState string

const (
	// ConsensusStoreNotReady indicates a ConsensusStore is not yet ready
	ConsensusStoreNotReady ConsensusStoreState = "NotReady"
	// ConsensusStoreReady indicates a ConsensusStore is ready
	ConsensusStoreReady ConsensusStoreState = "Ready"
)

type ConsensusStoreStatus struct {
	State ConsensusStoreState `json:"state,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConsensusStore is the Schema for the ConsensusStore API
// +k8s:openapi-gen=true
type ConsensusStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ConsensusStoreSpec   `json:"spec,omitempty"`
	Status            ConsensusStoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConsensusStoreList contains a list of ConsensusStore
type ConsensusStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the ConsensusStore of items in the list
	Items []ConsensusStore `json:"items"`
}
