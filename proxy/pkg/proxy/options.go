// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"github.com/atomix/runtime/sdk/pkg/runtime"
)

const (
	defaultRuntimePort = 5678
	defaultProxyPort   = 5679
)

type Options struct {
	RouterConfig   RouterConfig
	RuntimeService RuntimeServiceOptions
	ProxyService   ProxyServiceOptions
	Drivers        []runtime.Driver
}

func (o *Options) apply(opts ...Option) {
	o.RuntimeService.Port = defaultRuntimePort
	o.ProxyService.Port = defaultProxyPort
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*Options)

type ServerOptions struct {
	Host string
	Port int
}

type RuntimeServiceOptions struct {
	ServerOptions
	Types []runtime.Type
}

type ProxyServiceOptions struct {
	ServerOptions
}

func WithOptions(opts Options) Option {
	return func(options *Options) {
		*options = opts
	}
}

func WithRouterConfig(config RouterConfig) Option {
	return func(options *Options) {
		options.RouterConfig = config
	}
}

func WithDrivers(drivers ...runtime.Driver) Option {
	return func(options *Options) {
		options.Drivers = append(options.Drivers, drivers...)
	}
}

func WithTypes(types ...runtime.Type) Option {
	return func(options *Options) {
		options.RuntimeService.Types = append(options.RuntimeService.Types, types...)
	}
}

func WithRuntimeHost(host string) Option {
	return func(options *Options) {
		options.RuntimeService.Host = host
	}
}

func WithRuntimePort(port int) Option {
	return func(options *Options) {
		options.RuntimeService.Port = port
	}
}

func WithProxyHost(host string) Option {
	return func(options *Options) {
		options.ProxyService.Host = host
	}
}

func WithProxyPort(port int) Option {
	return func(options *Options) {
		options.ProxyService.Port = port
	}
}
