// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"time"
)

// WithPerCallTimeout sets the per-call retry timeout
func WithPerCallTimeout(t time.Duration) CallOption {
	return newCallOption(func(opts *callOptions) {
		opts.perCallTimeout = &t
	})
}

// WithInterval sets the base retry interval
func WithInterval(d time.Duration) CallOption {
	return newCallOption(func(opts *callOptions) {
		opts.initialInterval = &d
	})
}

// WithMaxInterval sets the maximum retry interval
func WithMaxInterval(d time.Duration) CallOption {
	return newCallOption(func(opts *callOptions) {
		opts.maxInterval = &d
	})
}

// WithRetryOn sets the codes on which to retry a request
func WithRetryOn(codes ...codes.Code) CallOption {
	return newCallOption(func(opts *callOptions) {
		opts.codes = codes
	})
}

func newCallOption(f func(opts *callOptions)) CallOption {
	return CallOption{
		applyFunc: f,
	}
}

// CallOption is a retrying interceptor call option
type CallOption struct {
	grpc.EmptyCallOption // make sure we implement private after() and before() fields so we don't panic.
	applyFunc            func(opts *callOptions)
}

type callOptions struct {
	perCallTimeout  *time.Duration
	initialInterval *time.Duration
	maxInterval     *time.Duration
	codes           []codes.Code
}

func newCallContext(ctx context.Context, opts *callOptions) context.Context {
	if opts.perCallTimeout != nil {
		ctx, _ = context.WithTimeout(ctx, *opts.perCallTimeout) //nolint:govet
	}
	return ctx
}

func newCallOptions(opts *callOptions, options []CallOption) *callOptions {
	if len(options) == 0 {
		return opts
	}
	optCopy := &callOptions{}
	*optCopy = *opts
	for _, f := range options {
		f.applyFunc(optCopy)
	}
	return optCopy
}

func filterCallOptions(options []grpc.CallOption) (grpcOptions []grpc.CallOption, retryOptions []CallOption) {
	for _, opt := range options {
		if co, ok := opt.(CallOption); ok {
			retryOptions = append(retryOptions, co)
		} else {
			grpcOptions = append(grpcOptions, opt)
		}
	}
	return grpcOptions, retryOptions
}
