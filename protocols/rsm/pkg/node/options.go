// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import "google.golang.org/grpc"

const (
	defaultPort = 8080
)

type Options struct {
	Host              string
	Port              int
	GRPCServerOptions []grpc.ServerOption
}

func (o *Options) apply(opts ...Option) {
	o.Port = defaultPort
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*Options)

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

func WithGRPCServerOptions(opts ...grpc.ServerOption) Option {
	return func(options *Options) {
		options.GRPCServerOptions = append(options.GRPCServerOptions, opts...)
	}
}
