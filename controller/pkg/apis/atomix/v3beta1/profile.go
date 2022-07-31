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

// Profile is a specification for a Profile resource
type Profile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ProfileSpec `json:"spec"`
}

// ProfileSpec is the spec for a Profile resource
type ProfileSpec struct {
	Routes []ProfileRoute `json:"routes"`
}

type ProfileRoute struct {
	Store    corev1.ObjectReference `json:"store"`
	Bindings []ProfileBinding       `json:"bindings"`
}

type ProfileBinding struct {
	Services   []ServiceConfig      `json:"services"`
	MatchRules []ProfileBindingRule `json:"matchRules"`
}

type ServiceConfig struct {
	Name   string               `json:"name"`
	Config runtime.RawExtension `json:"config"`
}

type ProfileBindingRule struct {
	Names []string          `json:"names"`
	Tags  map[string]string `json:"tags"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ProfileList is a list of Profile resources
type ProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Profile `json:"items"`
}
