// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import "google.golang.org/grpc"

type Options struct {
	GRPCDialOptions []grpc.DialOption
}

func (o *Options) apply(opts ...Option) {
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

func WithGRPCDialOptions(opts ...grpc.DialOption) Option {
	return func(options *Options) {
		options.GRPCDialOptions = append(options.GRPCDialOptions, opts...)
	}
}
