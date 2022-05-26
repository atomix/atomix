// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package atomix

import "github.com/atomix/runtime/pkg/atomix/primitive"

type Options struct {
	Host           string
	Port           int
	PrimitiveKinds []primitive.Kind
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

func WithPrimitiveKinds(kinds ...primitive.Kind) Option {
	return func(options *Options) {
		options.PrimitiveKinds = append(options.PrimitiveKinds, kinds...)
	}
}
