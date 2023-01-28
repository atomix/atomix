// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v3beta4

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageProfile is a specification for a StorageProfile resource
type StorageProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorageProfileSpec   `json:"spec"`
	Status StorageProfileStatus `json:"status"`
}

// StorageProfileSpec is the spec for a StorageProfile resource
type StorageProfileSpec struct {
	Routes []Route `json:"routes"`
}

type Route struct {
	Store corev1.ObjectReference `json:"store"`
	Rules []RoutingRule          `json:"rules"`
}

type RoutingRule struct {
	Kind       string               `json:"kind"`
	APIVersion string               `json:"apiVersion"`
	Names      []string             `json:"names"`
	Tags       []string             `json:"tags"`
	Config     runtime.RawExtension `json:"config"`
}

type StorageProfileStatus struct {
	PodStatuses []PodStatus `json:"podStatuses,omitempty"`
}

type PodStatus struct {
	corev1.ObjectReference `json:",inline"`
	Runtime                RuntimeStatus `json:"runtime"`
}

type RuntimeStatus struct {
	Routes []RouteStatus `json:"routes"`
}

type RouteState string

const (
	RoutePending       RouteState = "Pending"
	RouteConnecting    RouteState = "Connecting"
	RouteConnected     RouteState = "Connected"
	RouteConfiguring   RouteState = "Configuring"
	RouteDisconnecting RouteState = "Disconnecting"
	RouteDisconnected  RouteState = "Disconnected"
)

type RouteStatus struct {
	Store   corev1.ObjectReference `json:"store"`
	State   RouteState             `json:"state"`
	Version string                 `json:"version"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageProfileList is a list of StorageProfile resources
type StorageProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []StorageProfile `json:"items"`
}
