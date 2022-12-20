// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodMemoryStoreState is a state constant for PodMemoryStore
type PodMemoryStoreState string

const (
	// PodMemoryStoreNotReady indicates a PodMemoryStore is not yet ready
	PodMemoryStoreNotReady PodMemoryStoreState = "NotReady"
	// PodMemoryStoreReady indicates a PodMemoryStore is ready
	PodMemoryStoreReady PodMemoryStoreState = "Ready"
)

// PodMemoryStoreSpec specifies a PodMemoryStore configuration
type PodMemoryStoreSpec struct{}

// PodMemoryStoreStatus defines the status of a PodMemoryStore
type PodMemoryStoreStatus struct {
	State PodMemoryStoreState `json:"state,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMemoryStore is the Schema for the PodMemoryStore API
// +k8s:openapi-gen=true
type PodMemoryStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodMemoryStoreSpec   `json:"spec,omitempty"`
	Status            PodMemoryStoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMemoryStoreList contains a list of PodMemoryStore
type PodMemoryStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the PodMemoryStore of items in the list
	Items []PodMemoryStore `json:"items"`
}
