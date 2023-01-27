// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package v3beta3

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
	Proxy    StorageProxySpec `json:"proxy"`
	Bindings []Binding        `json:"bindings"`
}

type StorageProxySpec struct {
	// Image is the proxy image
	Image string `json:"image,omitempty"`

	// ImagePullPolicy is the pull policy to apply
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// SecurityContext is a pod security context
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// Logging is the proxy logging configuration
	Logging LoggingConfig `json:"logging,omitempty"`
}

type Binding struct {
	Store    corev1.ObjectReference `json:"store"`
	Priority *uint32                `json:"priority"`
	// Deprecated: use MatchTags instead
	Tags       []string        `json:"tags"`
	MatchTags  []string        `json:"matchTags"`
	Primitives []PrimitiveSpec `json:"primitives"`
}

type PrimitiveSpec struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Name       string `json:"name"`
	// Deprecated: use MatchTags instead
	Tags      []string             `json:"tags"`
	MatchTags []string             `json:"matchTags"`
	Config    runtime.RawExtension `json:"config"`
}

// LoggingConfig logging configuration
type LoggingConfig struct {
	Encoding  string         `json:"encoding"`
	RootLevel string         `json:"rootLevel"`
	Loggers   []LoggerConfig `json:"loggers"`
}

// LoggerConfig is the configuration for a logger
type LoggerConfig struct {
	Name  string  `json:"name"`
	Level *string `json:"level"`
}

type StorageProfileStatus struct {
	PodStatuses []PodStatus `json:"podStatuses,omitempty"`
}

type PodStatus struct {
	corev1.ObjectReference `json:",inline"`
	Proxy                  ProxyStatus `json:"proxy"`
}

type ProxyStatus struct {
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
