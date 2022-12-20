// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

type Config struct {
	Server ServerConfig `json:"server" yaml:"server"`
}

type ServerConfig struct {
	ReadBufferSize       *int    `json:"readBufferSize" yaml:"readBufferSize"`
	WriteBufferSize      *int    `json:"writeBufferSize" yaml:"writeBufferSize"`
	MaxRecvMsgSize       *int    `json:"maxRecvMsgSize" yaml:"maxRecvMsgSize"`
	MaxSendMsgSize       *int    `json:"maxSendMsgSize" yaml:"maxSendMsgSize"`
	NumStreamWorkers     *uint32 `json:"numStreamWorkers" yaml:"numStreamWorkers"`
	MaxConcurrentStreams *uint32 `json:"maxConcurrentStreams" yaml:"maxConcurrentStreams"`
}
