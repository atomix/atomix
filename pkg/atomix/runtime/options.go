// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package runtime

type Options struct {
	ConfigFile string
	CacheDir   string
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
