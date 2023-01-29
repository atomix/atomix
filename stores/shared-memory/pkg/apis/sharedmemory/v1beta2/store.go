// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SharedMemoryStoreState is a state constant for SharedMemoryStore
type SharedMemoryStoreState string

const (
	// SharedMemoryStoreNotReady indicates a SharedMemoryStore is not yet ready
	SharedMemoryStoreNotReady SharedMemoryStoreState = "NotReady"
	// SharedMemoryStoreReady indicates a SharedMemoryStore is ready
	SharedMemoryStoreReady SharedMemoryStoreState = "Ready"
)

// SharedMemoryStoreSpec specifies a SharedMemoryStore configuration
type SharedMemoryStoreSpec struct {
	// Image is the image to run
	Image string `json:"image,omitempty"`

	// ImagePullPolicy is the pull policy to apply
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is a list of secrets for pulling images
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// SecurityContext is a pod security context
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// Logging is the store logging configuration
	Logging LoggingConfig `json:"logging,omitempty"`
}

// LoggingConfig logging configuration
type LoggingConfig struct {
	Encoding  *string        `json:"encoding"`
	RootLevel *string        `json:"rootLevel"`
	Loggers   []LoggerConfig `json:"loggers"`
}

// LoggerConfig is the configuration for a logger
type LoggerConfig struct {
	Name  string  `json:"name"`
	Level *string `json:"level"`
}

// SharedMemoryStoreStatus defines the status of a SharedMemoryStore
type SharedMemoryStoreStatus struct {
	State SharedMemoryStoreState `json:"state,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SharedMemoryStore is the Schema for the SharedMemoryStore API
// +k8s:openapi-gen=true
type SharedMemoryStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SharedMemoryStoreSpec   `json:"spec,omitempty"`
	Status            SharedMemoryStoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SharedMemoryStoreList contains a list of SharedMemoryStore
type SharedMemoryStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the SharedMemoryStore of items in the list
	Items []SharedMemoryStore `json:"items"`
}
