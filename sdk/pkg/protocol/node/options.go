// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

const (
	defaultPort = 8080
)

type Options struct {
	ServerOptions
}

func (o *Options) apply(opts ...Option) {
	o.Port = defaultPort
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*Options)

type ServerOptions struct {
	Host string
	Port int
}

func WithOptions(opts Options) Option {
	return func(options *Options) {
		*options = opts
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
