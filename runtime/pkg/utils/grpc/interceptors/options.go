// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package interceptors

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"time"
)

// WithPerCallTimeout sets the per-call retry timeout
func WithPerCallTimeout(t time.Duration) RetryingCallOption {
	return newRetryingCallOption(func(opts *retryingCallOptions) {
		opts.perCallTimeout = &t
	})
}

// WithRetryDelay sets the base retry delay
func WithRetryDelay(d time.Duration) RetryingCallOption {
	return newRetryingCallOption(func(opts *retryingCallOptions) {
		opts.initialDelay = &d
	})
}

// WithMaxRetryDelay sets the maximum retry delay
func WithMaxRetryDelay(d time.Duration) RetryingCallOption {
	return newRetryingCallOption(func(opts *retryingCallOptions) {
		opts.maxDelay = &d
	})
}

// WithRetryOn sets the codes on which to retry a request
func WithRetryOn(codes ...codes.Code) RetryingCallOption {
	return newRetryingCallOption(func(opts *retryingCallOptions) {
		opts.codes = codes
	})
}

func newRetryingCallOption(f func(opts *retryingCallOptions)) RetryingCallOption {
	return RetryingCallOption{
		applyFunc: f,
	}
}

// RetryingCallOption is a retrying interceptor call option
type RetryingCallOption struct {
	grpc.EmptyCallOption // make sure we implement private after() and before() fields so we don't panic.
	applyFunc            func(opts *retryingCallOptions)
}

type retryingCallOptions struct {
	perCallTimeout *time.Duration
	initialDelay   *time.Duration
	maxDelay       *time.Duration
	codes          []codes.Code
}

func newCallContext(ctx context.Context, opts *retryingCallOptions) context.Context {
	if opts.perCallTimeout != nil {
		ctx, _ = context.WithTimeout(ctx, *opts.perCallTimeout) //nolint:govet
	}
	return ctx
}

func newCallOptions(opts *retryingCallOptions, options []RetryingCallOption) *retryingCallOptions {
	if len(options) == 0 {
		return opts
	}
	optCopy := &retryingCallOptions{}
	*optCopy = *opts
	for _, f := range options {
		f.applyFunc(optCopy)
	}
	return optCopy
}

func filterCallOptions(options []grpc.CallOption) (grpcOptions []grpc.CallOption, retryOptions []RetryingCallOption) {
	for _, opt := range options {
		if co, ok := opt.(RetryingCallOption); ok {
			retryOptions = append(retryOptions, co)
		} else {
			grpcOptions = append(grpcOptions, opt)
		}
	}
	return grpcOptions, retryOptions
}
