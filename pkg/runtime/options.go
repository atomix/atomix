// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import "github.com/atomix/sdk/pkg/controller"

type Options struct {
	Host       string
	Port       int
	Headless   bool
	Controller controller.Options
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

func WithHeadless() Option {
	return func(options *Options) {
		options.Headless = true
	}
}

func WithControllerHost(host string) Option {
	return func(options *Options) {
		options.Controller.Host = host
	}
}

func WithControllerPort(port int) Option {
	return func(options *Options) {
		options.Controller.Port = port
	}
}
