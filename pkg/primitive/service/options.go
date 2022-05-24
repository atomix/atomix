// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package service

import "github.com/atomix/runtime/pkg/primitive"

type Options struct {
	Host           string
	Port           int
	PrimitiveTypes []primitive.Type
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

func WithPrimitiveTypes(primitiveTypes ...primitive.Type) Option {
	return func(options *Options) {
		options.PrimitiveTypes = append(options.PrimitiveTypes, primitiveTypes...)
	}
}
