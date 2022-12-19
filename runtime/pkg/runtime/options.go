// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	runtimev1 "github.com/atomix/atomix/api/pkg/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/net"
	"google.golang.org/grpc"
)

const (
	defaultPort = 5679
)

type Options struct {
	ServiceOptions
	DriverProvider DriverProvider
	RouteProvider  RouteProvider
}

type ServiceOptions struct {
	Network           net.Network
	Host              string
	Port              int
	GRPCServerOptions []grpc.ServerOption
}

func (o *Options) apply(opts ...Option) {
	o.Network = net.NewNetwork()
	o.Port = defaultPort
	o.DriverProvider = newStaticDriverProvider()
	o.RouteProvider = newStaticRouteProvider()
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

func WithDriverProvider(provider DriverProvider) Option {
	return func(options *Options) {
		options.DriverProvider = provider
	}
}

func WithDrivers(drivers ...Driver) Option {
	return func(options *Options) {
		options.DriverProvider = newStaticDriverProvider(drivers...)
	}
}

func WithRouteProvider(provider RouteProvider) Option {
	return func(options *Options) {
		options.RouteProvider = provider
	}
}

func WithRoutes(routes ...*runtimev1.Route) Option {
	return func(options *Options) {
		options.RouteProvider = newStaticRouteProvider(routes...)
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
