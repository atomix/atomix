// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"github.com/atomix/runtime/pkg/primitive"
)

type Options struct {
	ProxyServiceOptions
	RuntimeServiceOptions
	ConfigFile string
	CacheDir   string
}

type ProxyServiceOptions struct {
	ProxyHost  string
	ProxyPort  int
	Primitives []primitive.Type
}

type RuntimeServiceOptions struct {
	RuntimeHost string
	RuntimePort int
}

func (o Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(&o)
	}
}

type Option func(*Options)

func WithOptions(opts Options) Option {
	return func(options *Options) {
		*options = opts
	}
}

func WithProxyHost(host string) Option {
	return func(options *Options) {
		options.ProxyHost = host
	}
}

func WithProxyPort(port int) Option {
	return func(options *Options) {
		options.ProxyPort = port
	}
}

func WithRuntimeHost(host string) Option {
	return func(options *Options) {
		options.RuntimeHost = host
	}
}

func WithRuntimePort(port int) Option {
	return func(options *Options) {
		options.RuntimePort = port
	}
}

func WithConfigFile(file string) Option {
	return func(options *Options) {
		options.ConfigFile = file
	}
}

func WithCacheDir(dir string) Option {
	return func(options *Options) {
		options.CacheDir = dir
	}
}

func WithPrimitives(atoms ...primitive.Type) Option {
	return func(options *Options) {
		options.Primitives = append(options.Primitives, atoms...)
	}
}
