// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	// Config is the memory store configuration
	Config SharedMemoryStoreConfig `json:"config,omitempty"`
}

type SharedMemoryStoreConfig struct {
	// Server is the memory server configuration
	Server SharedMemoryServerConfig `json:"server,omitempty"`

	// Logging is the store logging configuration
	Logging LoggingConfig `json:"logging,omitempty"`
}

type SharedMemoryServerConfig struct {
	ReadBufferSize       *int               `json:"readBufferSize"`
	WriteBufferSize      *int               `json:"writeBufferSize"`
	MaxRecvMsgSize       *resource.Quantity `json:"maxRecvMsgSize"`
	MaxSendMsgSize       *resource.Quantity `json:"maxSendMsgSize"`
	NumStreamWorkers     *uint32            `json:"numStreamWorkers"`
	MaxConcurrentStreams *uint32            `json:"maxConcurrentStreams"`
}

// LoggingConfig logging configuration
type LoggingConfig struct {
	Loggers map[string]LoggerConfig `json:"loggers" yaml:"loggers"`
	Sinks   map[string]SinkConfig   `json:"sinks" yaml:"sinks"`
}

// LoggerConfig is the configuration for a logger
type LoggerConfig struct {
	Level  *string                 `json:"level,omitempty" yaml:"level,omitempty"`
	Output map[string]OutputConfig `json:"output" yaml:"output"`
}

// OutputConfig is the configuration for a sink output
type OutputConfig struct {
	Sink  *string `json:"sink,omitempty" yaml:"sink,omitempty"`
	Level *string `json:"level,omitempty" yaml:"level,omitempty"`
}

// SinkConfig is the configuration for a sink
type SinkConfig struct {
	Encoding *string           `json:"encoding,omitempty" yaml:"encoding,omitempty"`
	Stdout   *StdoutSinkConfig `json:"stdout" yaml:"stdout,omitempty"`
	Stderr   *StderrSinkConfig `json:"stderr" yaml:"stderr,omitempty"`
	File     *FileSinkConfig   `json:"file" yaml:"file,omitempty"`
}

// StdoutSinkConfig is the configuration for an stdout sink
type StdoutSinkConfig struct {
}

// StderrSinkConfig is the configuration for an stderr sink
type StderrSinkConfig struct {
}

// FileSinkConfig is the configuration for a file sink
type FileSinkConfig struct {
	Path string `json:"path" yaml:"path"`
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
