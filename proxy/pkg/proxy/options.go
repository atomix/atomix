// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"github.com/atomix/atomix/api/pkg/driver"
)

const (
	defaultRuntimePort = 5678
	defaultProxyPort   = 5679
)

type Options struct {
	Config              Config
	ProxyService        ProxyServiceOptions
	ProxyControlService ProxyControlServiceOptions
	Drivers             []driver.Driver
	PluginsDir          string
}

func (o *Options) apply(opts ...Option) {
	o.ProxyService.Port = defaultRuntimePort
	o.ProxyControlService.Port = defaultProxyPort
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*Options)

type ServerOptions struct {
	Host string
	Port int
}

type ProxyServiceOptions struct {
	ServerOptions
	Types []Type
}

type ProxyControlServiceOptions struct {
	ServerOptions
}

func WithOptions(opts Options) Option {
	return func(options *Options) {
		*options = opts
	}
}

func WithConfig(config Config) Option {
	return func(options *Options) {
		options.Config = config
	}
}

func WithDrivers(drivers ...driver.Driver) Option {
	return func(options *Options) {
		options.Drivers = append(options.Drivers, drivers...)
	}
}

func WithPluginsDir(pluginsDir string) Option {
	return func(options *Options) {
		options.PluginsDir = pluginsDir
	}
}

func WithTypes(types ...Type) Option {
	return func(options *Options) {
		options.ProxyService.Types = append(options.ProxyService.Types, types...)
	}
}

func WithHost(host string) Option {
	return func(options *Options) {
		options.ProxyService.Host = host
	}
}

func WithPort(port int) Option {
	return func(options *Options) {
		options.ProxyService.Port = port
	}
}

func WithControlHost(host string) Option {
	return func(options *Options) {
		options.ProxyControlService.Host = host
	}
}

func WithControlPort(port int) Option {
	return func(options *Options) {
		options.ProxyControlService.Port = port
	}
}
