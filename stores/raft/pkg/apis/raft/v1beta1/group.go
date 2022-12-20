// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RaftGroupState is a state constant for RaftGroup
type RaftGroupState string

const (
	// RaftGroupNotReady indicates a RaftGroup is not yet ready
	RaftGroupNotReady RaftGroupState = "NotReady"
	// RaftGroupReady indicates a RaftGroup is ready
	RaftGroupReady RaftGroupState = "Ready"
)

// RaftGroupSpec specifies a RaftGroupSpec configuration
type RaftGroupSpec struct {
	RaftConfig `json:",inline"`
}

// RaftGroupStatus defines the status of a RaftGroup
type RaftGroupStatus struct {
	State     RaftGroupState                `json:"state,omitempty"`
	Term      *uint64                       `json:"term,omitempty"`
	Leader    *corev1.LocalObjectReference  `json:"leader,omitempty"`
	Followers []corev1.LocalObjectReference `json:"followers,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftGroup is the Schema for the RaftGroup API
// +k8s:openapi-gen=true
type RaftGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RaftGroupSpec   `json:"spec,omitempty"`
	Status            RaftGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RaftGroupList contains a list of RaftGroup
type RaftGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the RaftGroup of items in the list
	Items []RaftGroup `json:"items"`
}
