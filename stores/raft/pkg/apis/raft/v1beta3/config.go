// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta3

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// RaftConfig is the configuration of a Raft group
type RaftConfig struct {
	ElectionRTT        *int64             `json:"electionRTT,omitempty"`
	HeartbeatRTT       *int64             `json:"heartbeatRTT,omitempty"`
	SnapshotEntries    *int64             `json:"snapshotEntries,omitempty"`
	CompactionOverhead *int64             `json:"compactionOverhead,omitempty"`
	MaxInMemLogSize    *resource.Quantity `json:"maxInMemLogSize,omitempty"`
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
