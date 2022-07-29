// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v3beta1

import (
	corev1 "k8s.io/api/core/v1"
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

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Profile is a specification for a Profile resource
type Profile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ProfileSpec `json:"spec"`
}

// ProfileSpec is the spec for a Profile resource
type ProfileSpec struct {
	Bindings []ProfileBinding `json:"bindings"`
}

type ProfileBinding struct {
	Name       string                 `json:"name"`
	Store      corev1.ObjectReference `json:"store"`
	Primitives []PrimitiveBindingRule `json:"primitives"`
}

type PrimitiveBindingRule struct {
	Kinds       []string          `json:"kinds"`
	APIVersions []string          `json:"apiVersions"`
	Names       []string          `json:"names"`
	Tags        map[string]string `json:"tags"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ProfileList is a list of Profile resources
type ProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Profile `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Proxy is a specification for a Proxy resource
type Proxy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Pod     corev1.LocalObjectReference `json:"pod"`
	Profile corev1.LocalObjectReference `json:"profile"`
	Status  ProxyStatus                 `json:"status"`
}

type ProxyStatus struct {
	Ready    bool            `json:"ready"`
	Bindings []BindingStatus `json:"bindings"`
}

type BindingState string

const (
	BindingUnbound BindingState = "Unbound"
	BindingBound   BindingState = "Bound"
)

type BindingStatus struct {
	Name    string       `json:"name"`
	State   BindingState `json:"state"`
	Version string       `json:"version"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ProxyList is a list of Proxy resources
type ProxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Proxy `json:"items"`
}
