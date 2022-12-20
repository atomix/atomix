// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import "time"

const (
	defaultDataDir                 = "/var/lib/atomix/data"
	defaultSnapshotEntryThreshold  = uint64(10000)
	defaultCompactionRetainEntries = uint64(1000)
	defaultHeartbeatPeriod         = 200 * time.Millisecond
	defaultClientTimeout           = time.Minute
)

type Config struct {
	Server ServerConfig `json:"server" yaml:"server"`
	Raft   RaftConfig   `json:"raft" yaml:"raft"`
}

type ServerConfig struct {
	ReadBufferSize       *int    `json:"readBufferSize" yaml:"readBufferSize"`
	WriteBufferSize      *int    `json:"writeBufferSize" yaml:"writeBufferSize"`
	MaxRecvMsgSize       *int    `json:"maxRecvMsgSize" yaml:"maxRecvMsgSize"`
	MaxSendMsgSize       *int    `json:"maxSendMsgSize" yaml:"maxSendMsgSize"`
	NumStreamWorkers     *uint32 `json:"numStreamWorkers" yaml:"numStreamWorkers"`
	MaxConcurrentStreams *uint32 `json:"maxConcurrentStreams" yaml:"maxConcurrentStreams"`
}

type RaftConfig struct {
	HeartbeatPeriod         *time.Duration `json:"heartbeatPeriod" yaml:"heartbeatPeriod"`
	ElectionTimeout         *time.Duration `json:"electionTimeout" yaml:"electionTimeout"`
	SnapshotEntryThreshold  *uint64        `json:"snapshotEntryThreshold" yaml:"snapshotEntryThreshold"`
	CompactionRetainEntries *uint64        `json:"compactionRetainEntries" yaml:"compactionRetainEntries"`
	DataDir                 *string        `json:"dataDir" yaml:"dataDir"`
}

func (c RaftConfig) GetDataDir() string {
	if c.DataDir != nil {
		return *c.DataDir
	}
	return defaultDataDir
}

func (c RaftConfig) GetSnapshotEntryThreshold() uint64 {
	if c.SnapshotEntryThreshold != nil {
		return *c.SnapshotEntryThreshold
	}
	return defaultSnapshotEntryThreshold
}

func (c RaftConfig) GetCompactionRetainEntries() uint64 {
	if c.CompactionRetainEntries != nil {
		return *c.CompactionRetainEntries
	}
	return defaultCompactionRetainEntries
}

func (c RaftConfig) GetHeartbeatPeriod() time.Duration {
	if c.HeartbeatPeriod != nil {
		return *c.HeartbeatPeriod
	}
	return defaultHeartbeatPeriod
}
