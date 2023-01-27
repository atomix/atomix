// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1

import (
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/driver"
)

type Options struct {
	DriverProvider DriverProvider
	Drivers        map[runtimev1.DriverID]driver.Driver
}

func (o *Options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
	if o.DriverProvider == nil {
		o.DriverProvider = newStaticDriverProvider(o.Drivers)
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

func WithDriver(id runtimev1.DriverID, d driver.Driver) Option {
	return func(options *Options) {
		if options.Drivers == nil {
			options.Drivers = make(map[runtimev1.DriverID]driver.Driver)
		}
		options.Drivers[id] = d
	}
}

func WithDrivers(drivers map[runtimev1.DriverID]driver.Driver) Option {
	return func(options *Options) {
		options.DriverProvider = newStaticDriverProvider(drivers)
	}
}
