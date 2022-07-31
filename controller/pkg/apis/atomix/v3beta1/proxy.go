// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v3beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
	Ready  bool          `json:"ready"`
	Routes []RouteStatus `json:"routes"`
}

type RouteState string

const (
	RoutePending   RouteState = "Pending"
	RouteConnected RouteState = "Connected"
)

type RouteStatus struct {
	Store   corev1.ObjectReference `json:"store"`
	State   RouteState             `json:"state"`
	Version string                 `json:"version"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ProxyList is a list of Proxy resources
type ProxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Proxy `json:"items"`
}
