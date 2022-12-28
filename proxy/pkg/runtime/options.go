// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"github.com/atomix/atomix/runtime/pkg/network"
)

const (
	defaultPort = 5679
)

type Options struct {
	ServiceOptions
}

func (o *Options) apply(opts ...Option) {
	o.Network = network.NewDefaultDriver()
	o.Port = defaultPort
	for _, opt := range opts {
		opt(o)
	}
}

type Option = func(*Options)

type ServiceOptions struct {
	Network network.Driver
	Host    string
	Port    int
}

func WithOptions(opts Options) Option {
	return func(options *Options) {
		*options = opts
	}
}

func WithNetwork(driver network.Driver) Option {
	return func(options *Options) {
		options.Network = driver
	}
}

func WithHost(host string) Option {
	return func(options *Options) {
		options.Host = host
	}
}

func WithPort(port int) Option {
	return func(options *Options) {
		options.Port = port
	}
}
