// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v3beta4

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DataStore is a specification for a DataStore resource
type DataStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec DataStoreSpec `json:"spec"`
}

// DataStoreSpec is the spec for a DataStore resource
type DataStoreSpec struct {
	Driver Driver               `json:"driver"`
	Config runtime.RawExtension `json:"config"`
}

type Driver struct {
	Name       string `json:"name"`
	APIVersion string `json:"apiVersion"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DataStoreList is a list of DataStore resources
type DataStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []DataStore `json:"items"`
}
