// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v3beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Store is a specification for a Store resource
type Store struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec StoreSpec `json:"spec"`
}

// StoreSpec is the spec for a Store resource
type StoreSpec struct {
	Driver Driver               `json:"driver"`
	Config runtime.RawExtension `json:"config"`
}

type Driver struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StoreList is a list of Store resources
type StoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Store `json:"items"`
}
