// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"github.com/atomix/atomix/runtime/pkg/net"
	"google.golang.org/grpc"
)

const (
	defaultPort = 5679
)

type Options struct {
	Network           net.Network
	Host              string
	Port              int
	GRPCServerOptions []grpc.ServerOption
}

func (o *Options) apply(opts ...Option) {
	o.Network = net.NewNetwork()
	o.Port = defaultPort
	for _, opt := range opts {
		opt(o)
	}
}

type Option = func(*Options)

func WithOptions(opts Options) Option {
	return func(options *Options) {
		*options = opts
	}
}

func WithNetwork(network net.Network) Option {
	return func(options *Options) {
		options.Network = network
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

func WithServerOption(opt grpc.ServerOption) Option {
	return WithServerOptions(opt)
}

func WithServerOptions(opts ...grpc.ServerOption) Option {
	return func(options *Options) {
		options.GRPCServerOptions = append(options.GRPCServerOptions, opts...)
	}
}
