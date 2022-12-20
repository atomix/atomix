// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package raft

import "time"

const (
	defaultDataDir            = "/var/lib/atomix/data"
	defaultRTT                = 10 * time.Millisecond
	defaultElectionRTT        = 100
	defaultHeartbeatRTT       = 10
	defaultSnapshotEntries    = uint64(10000)
	defaultCompactionOverhead = uint64(1000)
	defaultClientTimeout      = time.Minute
)

type Config struct {
	Server ServerConfig `json:"server" yaml:"server"`
	Node   NodeConfig   `json:"node" yaml:"node"`
}

type ServerConfig struct {
	ReadBufferSize       *int    `json:"readBufferSize" yaml:"readBufferSize"`
	WriteBufferSize      *int    `json:"writeBufferSize" yaml:"writeBufferSize"`
	MaxRecvMsgSize       *int    `json:"maxRecvMsgSize" yaml:"maxRecvMsgSize"`
	MaxSendMsgSize       *int    `json:"maxSendMsgSize" yaml:"maxSendMsgSize"`
	NumStreamWorkers     *uint32 `json:"numStreamWorkers" yaml:"numStreamWorkers"`
	MaxConcurrentStreams *uint32 `json:"maxConcurrentStreams" yaml:"maxConcurrentStreams"`
}

type NodeConfig struct {
	RTT     *time.Duration `json:"rtt" yaml:"rtt"`
	DataDir *string        `json:"dataDir" yaml:"dataDir"`
}

func (c NodeConfig) GetDataDir() string {
	if c.DataDir != nil {
		return *c.DataDir
	}
	return defaultDataDir
}

func (c NodeConfig) GetRTT() time.Duration {
	if c.RTT != nil {
		return *c.RTT
	}
	return defaultRTT
}
