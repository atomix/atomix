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
	defaultControllerPort = 5679
)

type Options struct {
	DriverProvider DriverProvider
	RouteProvider  RouteProvider
}

func (o *Options) apply(opts ...Option) {
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

type ServerOptions struct {
	Network           net.Network
	Host              string
	Port              int
	GRPCServerOptions []grpc.ServerOption
}

type ControllerOptions struct {
	ServerOptions
}

func (o *ControllerOptions) apply(opts ...ControllerOption) {
	o.Network = net.NewNetwork()
	o.Port = defaultControllerPort
	for _, opt := range opts {
		opt(o)
	}
}

type ControllerOption = func(*ControllerOptions)

func WithControllerOptions(opts ControllerOptions) ControllerOption {
	return func(options *ControllerOptions) {
		*options = opts
	}
}

func WithControllerNetwork(network net.Network) ControllerOption {
	return func(options *ControllerOptions) {
		options.Network = network
	}
}

func WithControllerHost(host string) ControllerOption {
	return func(options *ControllerOptions) {
		options.Host = host
	}
}

func WithControllerPort(port int) ControllerOption {
	return func(options *ControllerOptions) {
		options.Port = port
	}
}

func WithServerOption(opt grpc.ServerOption) ControllerOption {
	return WithServerOptions(opt)
}

func WithServerOptions(opts ...grpc.ServerOption) ControllerOption {
	return func(options *ControllerOptions) {
		options.GRPCServerOptions = append(options.GRPCServerOptions, opts...)
	}
}
