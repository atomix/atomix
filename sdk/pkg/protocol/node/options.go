// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package node

import "google.golang.org/grpc"

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
	Host     string
	Port     int
	Services []func(*grpc.Server)
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

func WithService(service func(*grpc.Server)) Option {
	return func(options *Options) {
		options.Services = append(options.Services, service)
	}
}

func WithServices(services ...func(*grpc.Server)) Option {
	return func(options *Options) {
		options.Services = append(options.Services, services...)
	}
}
