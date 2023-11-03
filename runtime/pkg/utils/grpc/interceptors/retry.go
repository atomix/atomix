// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package interceptors

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/atomix/atomix/runtime/pkg/logging"
)

var log = logging.GetLogger()

var defaultOptions = &retryingCallOptions{
	codes: []codes.Code{
		codes.Unavailable,
	},
}

// RetryingUnaryClientInterceptor returns a UnaryClientInterceptor that retries requests
func RetryingUnaryClientInterceptor(callOpts ...RetryingCallOption) func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	connOpts := newCallOptions(defaultOptions, callOpts)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		grpcOpts, retryOpts := filterCallOptions(opts)
		callOpts := newCallOptions(connOpts, retryOpts)
		retryPolicy := retryPolicyBuilder(callOpts.initialDelay, callOpts.maxDelay).Build()
		return failsafe.Run(func() error {
			log.Debugf("SendMsg %.250s", req)
			callCtx := newCallContext(ctx, callOpts)
			if err := invoker(callCtx, method, req, reply, cc, grpcOpts...); err != nil {
				if isContextError(err) {
					if ctx.Err() != nil {
						log.Debugf("SendMsg %.250s: error", req, err)
						return NewPermanentError(err)
					} else if callOpts.perCallTimeout != nil {
						log.Debugf("SendMsg %.250s: error", req, err)
						return err
					}
				}
				if isRetryable(callOpts, err) {
					log.Debugf("SendMsg %.250s: error", req, err)
					return err
				}
				log.Warnf("SendMsg %.250s: error", req, err)
				return NewPermanentError(err)
			}
			log.Debugf("RecvMsg %.250s", reply)
			return nil
		}, retryPolicy)
	}
}

// RetryingStreamClientInterceptor returns a ClientStreamInterceptor that retries both requests and responses
func RetryingStreamClientInterceptor(callOpts ...RetryingCallOption) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if desc.ClientStreams && desc.ServerStreams {
			return newBiDirectionalStreamClientInterceptor(callOpts...)(ctx, desc, cc, method, streamer, opts...)
		} else if desc.ClientStreams {
			return newClientStreamClientInterceptor(callOpts...)(ctx, desc, cc, method, streamer, opts...)
		} else if desc.ServerStreams {
			return newServerStreamClientInterceptor(callOpts...)(ctx, desc, cc, method, streamer, opts...)
		}
		panic("Invalid StreamDesc")
	}
}

// newClientStreamClientInterceptor returns a ClientStreamInterceptor that retries both requests and responses
func newClientStreamClientInterceptor(callOpts ...RetryingCallOption) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	connOpts := newCallOptions(defaultOptions, callOpts)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		grpcOpts, retryOpts := filterCallOptions(opts)
		callOpts := newCallOptions(connOpts, retryOpts)
		stream := &retryingClientStream{
			ctx:    ctx,
			buffer: &retryingClientStreamBuffer{},
			opts:   callOpts,
			newStream: func(ctx context.Context) (grpc.ClientStream, error) {
				return streamer(ctx, desc, cc, method, grpcOpts...)
			},
		}
		return stream, stream.retryStream()
	}
}

// newServerStreamClientInterceptor returns a ClientStreamInterceptor that retries both requests and responses
func newServerStreamClientInterceptor(callOpts ...RetryingCallOption) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	connOpts := newCallOptions(defaultOptions, callOpts)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		grpcOpts, retryOpts := filterCallOptions(opts)
		callOpts := newCallOptions(connOpts, retryOpts)
		stream := &retryingClientStream{
			ctx:    ctx,
			buffer: &retryingServerStreamBuffer{},
			opts:   callOpts,
			newStream: func(ctx context.Context) (grpc.ClientStream, error) {
				return streamer(ctx, desc, cc, method, grpcOpts...)
			},
		}
		return stream, stream.retryStream()
	}
}

// newBiDirectionalStreamClientInterceptor returns a ClientStreamInterceptor that retries both requests and responses
func newBiDirectionalStreamClientInterceptor(callOpts ...RetryingCallOption) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	connOpts := newCallOptions(defaultOptions, callOpts)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		grpcOpts, retryOpts := filterCallOptions(opts)
		callOpts := newCallOptions(connOpts, retryOpts)
		stream := &retryingClientStream{
			ctx:    ctx,
			buffer: &retryingBiDirectionalStreamBuffer{},
			opts:   callOpts,
			newStream: func(ctx context.Context) (grpc.ClientStream, error) {
				return streamer(ctx, desc, cc, method, grpcOpts...)
			},
		}
		return stream, stream.retryStream()
	}
}

type retryingStreamBuffer interface {
	append(interface{})
	list() []interface{}
}

type retryingClientStreamBuffer struct {
	buffer []interface{}
	mu     sync.RWMutex
}

func (b *retryingClientStreamBuffer) append(msg interface{}) {
	b.mu.Lock()
	b.buffer = append(b.buffer, msg)
	b.mu.Unlock()
}

func (b *retryingClientStreamBuffer) list() []interface{} {
	b.mu.RLock()
	buffer := make([]interface{}, len(b.buffer))
	copy(buffer, b.buffer)
	b.mu.RUnlock()
	return buffer
}

type retryingServerStreamBuffer struct {
	msg interface{}
	mu  sync.RWMutex
}

func (b *retryingServerStreamBuffer) append(msg interface{}) {
	b.mu.Lock()
	b.msg = msg
	b.mu.Unlock()
}

func (b *retryingServerStreamBuffer) list() []interface{} {
	b.mu.RLock()
	msg := b.msg
	b.mu.RUnlock()
	if msg != nil {
		return []interface{}{msg}
	}
	return []interface{}{}
}

type retryingBiDirectionalStreamBuffer struct{}

func (b *retryingBiDirectionalStreamBuffer) append(interface{}) {

}

func (b *retryingBiDirectionalStreamBuffer) list() []interface{} {
	return []interface{}{}
}

type retryingClientStream struct {
	ctx       context.Context
	stream    grpc.ClientStream
	opts      *retryingCallOptions
	mu        sync.RWMutex
	buffer    retryingStreamBuffer
	newStream func(ctx context.Context) (grpc.ClientStream, error)
	closed    bool
}

func (s *retryingClientStream) getStream() grpc.ClientStream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stream
}

func (s *retryingClientStream) Context() context.Context {
	return s.ctx
}

func (s *retryingClientStream) CloseSend() error {
	log.Debug("CloseSend")
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	if err := s.getStream().CloseSend(); err != nil {
		log.Warn("CloseSend: error", err)
		return err
	}
	return nil
}

func (s *retryingClientStream) Header() (metadata.MD, error) {
	return s.getStream().Header()
}

func (s *retryingClientStream) Trailer() metadata.MD {
	return s.getStream().Trailer()
}

func (s *retryingClientStream) SendMsg(m interface{}) error {
	log.Debugf("SendMsg %.250s", m)
	return s.retrySendMsg(m)
}

func (s *retryingClientStream) retrySendMsg(m interface{}) error {
	retryPolicy := retryPolicyBuilder(nil, nil).
		OnRetryScheduled(func(f failsafe.ExecutionScheduledEvent[any]) {
			log.Debugf("SendMsg %.250s: retry after %.250s", m, f.Delay, f.LastError())
		}).
		Build()
	return failsafe.Run(func() error {
		return s.trySendMsg(m)
	}, retryPolicy)
}

func (s *retryingClientStream) trySendMsg(m interface{}) error {
	err := s.getStream().SendMsg(m)
	if err == nil {
		s.buffer.append(m)
		return nil
	}
	if isContextError(err) {
		if s.ctx.Err() != nil {
			log.Debugf("SendMsg %.250s: error", m, err)
			return NewPermanentError(err)
		} else if s.opts.perCallTimeout != nil {
			log.Debugf("SendMsg %.250s: error", m, err)
			if err := s.tryStream(); err != nil {
				log.Debug("SendMsg %.250s: error", m, err)
				return err
			}
			return s.trySendMsg(m)
		}
	}
	if isRetryable(s.opts, err) {
		log.Debugf("SendMsg %.250s: error", m, err)
		if err := s.tryStream(); err != nil {
			log.Debug("SendMsg %.250s: error", m, err)
			return err
		}
		return s.trySendMsg(m)
	}
	log.Warnf("SendMsg %.250s: error", m, err)
	return NewPermanentError(err)
}

func (s *retryingClientStream) RecvMsg(m interface{}) error {
	return s.retryRecvMsg(m)
}

func (s *retryingClientStream) retryRecvMsg(m interface{}) error {
	retryPolicy := retryPolicyBuilder(nil, nil).
		OnRetryScheduled(func(f failsafe.ExecutionScheduledEvent[any]) {
			log.Debugf("RecvMsg: retry after %s", f.Delay, f.LastError())
		}).
		Build()
	return failsafe.Run(func() error {
		return s.tryRecvMsg(m)
	}, retryPolicy)
}

func (s *retryingClientStream) tryRecvMsg(m interface{}) error {
	err := s.getStream().RecvMsg(m)
	if err == nil {
		log.Debugf("RecvMsg %.250s", m)
		return nil
	}
	if err == io.EOF {
		log.Debug("RecvMsg: EOF")
		return NewPermanentError(err)
	}
	if isContextError(err) {
		if s.ctx.Err() != nil {
			log.Debug("RecvMsg: error", err)
			return NewPermanentError(err)
		} else if s.opts.perCallTimeout != nil {
			log.Debug("RecvMsg: error", err)
			if err := s.tryStream(); err != nil {
				log.Debug("RecvMsg: error", err)
				return err
			}
			return s.tryRecvMsg(m)
		}
	}
	if isRetryable(s.opts, err) {
		log.Debug("RecvMsg: error", err)
		if err := s.tryStream(); err != nil {
			log.Debug("RecvMsg: error", err)
			return err
		}
		return s.tryRecvMsg(m)
	}
	log.Warn("RecvMsg: error", err)
	return NewPermanentError(err)
}

func (s *retryingClientStream) retryStream() error {
	retryPolicy := retryPolicyBuilder(s.opts.initialDelay, s.opts.maxDelay).
		OnRetryScheduled(func(f failsafe.ExecutionScheduledEvent[any]) {
			log.Debugf("Stream: retry after %s", f.Delay, f.LastError())
		}).
		Build()
	return failsafe.Run(s.tryStream, retryPolicy)
}

func (s *retryingClientStream) tryStream() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, err := s.newStream(newCallContext(s.ctx, s.opts))
	if err != nil {
		if isContextError(err) {
			if s.ctx.Err() != nil {
				log.Debug("Stream: error", err)
				return NewPermanentError(err)
			} else if s.opts.perCallTimeout != nil {
				log.Debug("Stream: error", err)
				return err
			}
		}
		if isRetryable(s.opts, err) {
			log.Debug("Stream: error", err)
			return err
		}
		log.Warn("Stream: error", err)
		return NewPermanentError(err)
	}

	msgs := s.buffer.list()
	for _, m := range msgs {
		log.Debugf("SendMsg %.250s", m)
		if err := stream.SendMsg(m); err != nil {
			if isContextError(err) {
				if s.ctx.Err() != nil {
					log.Debugf("SendMsg %.250s: error", m, err)
					return NewPermanentError(err)
				} else if s.opts.perCallTimeout != nil {
					log.Debugf("SendMsg %.250s: error", m, err)
					return err
				}
			}
			if isRetryable(s.opts, err) {
				log.Debugf("SendMsg %.250s: error", m, err)
				return err
			}
			log.Warnf("SendMsg %.250s: error", m, err)
			return NewPermanentError(err)
		}
	}

	if s.closed {
		log.Debug("CloseSend")
		if err := stream.CloseSend(); err != nil {
			if isContextError(err) {
				if s.ctx.Err() != nil {
					log.Debug("CloseSend: error", err)
					return NewPermanentError(err)
				} else if s.opts.perCallTimeout != nil {
					log.Debug("CloseSend: error", err)
					return err
				}
			}
			if isRetryable(s.opts, err) {
				log.Debug("CloseSend: error", err)
				return err
			}
			log.Warn("CloseSend: error", err)
			return NewPermanentError(err)
		}
	}
	s.stream = stream
	return nil
}

func isContextError(err error) bool {
	code := status.Code(err)
	return code == codes.DeadlineExceeded || code == codes.Canceled
}

func isRetryable(opts *retryingCallOptions, err error) bool {
	code := status.Code(err)
	if code == codes.Canceled || code == codes.DeadlineExceeded {
		return false
	}
	for _, retryableCode := range opts.codes {
		if code == retryableCode {
			return true
		}
	}
	return false
}

func retryPolicyBuilder(minDelay *time.Duration, maxDelay *time.Duration) retrypolicy.RetryPolicyBuilder[any] {
	minD := 500 * time.Millisecond
	if minDelay != nil {
		minD = *minDelay
	}
	maxD := time.Minute
	if maxDelay != nil {
		maxD = *maxDelay
	}
	return retrypolicy.Builder[any]().
		AbortOnErrors(&PermanentError{}).
		WithBackoffFactor(minD, maxD, 1.5).
		WithJitterFactor(.5).
		WithMaxDuration(15 * time.Minute)
}

// PermanentError signals that the operation should not be retried.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Is(err error) bool {
	_, ok := err.(*PermanentError)
	return ok
}

// NewPermanentError wraps the given err in a *PermanentError.
func NewPermanentError(err error) *PermanentError {
	return &PermanentError{
		Err: err,
	}
}
