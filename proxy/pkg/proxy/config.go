// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

type Config struct {
	Server ServerConfig `json:"server" yaml:"server"`
	Router RouterConfig `json:"router" yaml:"router"`
}

type ServerConfig struct {
	ReadBufferSize       *int    `json:"readBufferSize" yaml:"readBufferSize"`
	WriteBufferSize      *int    `json:"writeBufferSize" yaml:"writeBufferSize"`
	MaxRecvMsgSize       *int    `json:"maxRecvMsgSize" yaml:"maxRecvMsgSize"`
	MaxSendMsgSize       *int    `json:"maxSendMsgSize" yaml:"maxSendMsgSize"`
	NumStreamWorkers     *uint32 `json:"numStreamWorkers" yaml:"numStreamWorkers"`
	MaxConcurrentStreams *uint32 `json:"maxConcurrentStreams" yaml:"maxConcurrentStreams"`
}

type RouterConfig struct {
	Routes []RouteConfig `json:"routes" yaml:"routes"`
}

type StoreID struct {
	Namespace string `json:"namespace" yaml:"namespace"`
	Name      string `json:"name" yaml:"name"`
}

type RouteConfig struct {
	Store    StoreID           `json:"store" yaml:"store"`
	Selector map[string]string `json:"selector" yaml:"selector"`
	Services []ServiceConfig   `json:"services" yaml:"services"`
}

type ServiceConfig struct {
	Name   string                 `json:"name" yaml:"name"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}
