// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta2

import "k8s.io/apimachinery/pkg/api/resource"

// RaftConfig is the configuration of a Raft group
type RaftConfig struct {
	ElectionRTT        *uint64            `json:"electionRTT,omitempty"`
	HeartbeatRTT       *uint64            `json:"heartbeatRTT,omitempty"`
	SnapshotEntries    *int64             `json:"snapshotEntries,omitempty"`
	CompactionOverhead *int64             `json:"compactionOverhead,omitempty"`
	MaxInMemLogSize    *resource.Quantity `json:"maxInMemLogSize,omitempty"`
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
