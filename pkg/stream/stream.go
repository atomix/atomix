// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"container/list"
	"sync"
)

// ReadStream is a state machine read stream
type ReadStream[T any] interface {
	// Receive receives the next result
	Receive() (Result[T], bool)

	// Drain drains the stream
	Drain()
}

// WriteStream is a state machine write stream
type WriteStream[T any] interface {
	// Send sends an output on the stream
	Send(out Result[T])

	// Result sends a result on the stream
	Result(value T, err error)

	// Value sends a value on the stream
	Value(value T)

	// Error sends an error on the stream
	Error(err error)

	// Close closes the stream
	Close()
}

// Stream is a read/write stream
type Stream[T any] interface {
	ReadStream[T]
	WriteStream[T]
}

// NewUnaryStream returns a new read/write stream that expects one result
func NewUnaryStream[T any]() Stream[T] {
	return &unaryStream[T]{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// unaryStream is a stream that expects one result
type unaryStream[T any] struct {
	result *Result[T]
	closed bool
	cond   *sync.Cond
}

func (s *unaryStream[T]) Receive() (Result[T], bool) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.closed {
		return Result[T]{}, false
	}
	if s.result == nil {
		if s.closed {
			return Result[T]{}, false
		}
		s.cond.Wait()
	}
	result := s.result
	s.result = nil
	return *result, true
}

func (s *unaryStream[T]) Drain() {
	s.Close()
}

func (s *unaryStream[T]) Send(result Result[T]) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if !s.closed {
		s.result = &result
		s.closed = true
	}
	s.cond.Signal()
}

func (s *unaryStream[T]) Result(value T, err error) {
	s.Send(Result[T]{
		Value: value,
		Error: err,
	})
}

func (s *unaryStream[T]) Value(value T) {
	s.Send(Result[T]{
		Value: value,
	})
}

func (s *unaryStream[T]) Error(err error) {
	s.Send(Result[T]{
		Error: err,
	})
}

func (s *unaryStream[T]) Close() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	s.closed = true
}

// NewBufferedStream returns a new buffered read/write stream
func NewBufferedStream[T any]() Stream[T] {
	return &bufferedStream[T]{
		buffer: list.New(),
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

// bufferedStream is a buffered read/write stream
type bufferedStream[T any] struct {
	buffer *list.List
	closed bool
	cond   *sync.Cond
}

func (s *bufferedStream[T]) Receive() (Result[T], bool) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.buffer == nil {
		return Result[T]{}, false
	}
	for s.buffer.Len() == 0 {
		if s.closed {
			return Result[T]{}, false
		}
		s.cond.Wait()
	}
	result := s.buffer.Front().Value.(Result[T])
	s.buffer.Remove(s.buffer.Front())
	return result, true
}

func (s *bufferedStream[T]) Drain() {
	s.cond.L.Lock()
	defer s.cond.L.Lock()
	s.buffer = nil
}

func (s *bufferedStream[T]) Send(result Result[T]) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.buffer != nil {
		s.buffer.PushBack(result)
		s.cond.Signal()
	}
}

func (s *bufferedStream[T]) Result(value T, err error) {
	s.Send(Result[T]{
		Value: value,
		Error: err,
	})
}

func (s *bufferedStream[T]) Value(value T) {
	s.Send(Result[T]{
		Value: value,
	})
}

func (s *bufferedStream[T]) Error(err error) {
	s.Send(Result[T]{
		Error: err,
	})
}

func (s *bufferedStream[T]) Close() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if !s.closed {
		s.closed = true
		s.cond.Signal()
	}
}

// NewChannelStream returns a new channel-based stream
func NewChannelStream[T any](ch chan Result[T]) Stream[T] {
	return &channelStream[T]{
		ch: ch,
	}
}

// channelStream is a channel-based stream
type channelStream[T any] struct {
	ch chan Result[T]
}

func (s *channelStream[T]) Receive() (Result[T], bool) {
	result, ok := <-s.ch
	return result, ok
}

func (s *channelStream[T]) Drain() {
	go func() {
		for range s.ch {
		}
	}()
}

func (s *channelStream[T]) Send(result Result[T]) {
	s.ch <- result
}

func (s *channelStream[T]) Result(value T, err error) {
	s.Send(Result[T]{
		Value: value,
		Error: err,
	})
}

func (s *channelStream[T]) Value(value T) {
	s.Result(value, nil)
}

func (s *channelStream[T]) Error(err error) {
	s.Result(nil, err)
}

func (s *channelStream[T]) Close() {
	close(s.ch)
}

// NewNilStream returns a disconnected stream
func NewNilStream[T any]() WriteStream[T] {
	return &nilStream[T]{}
}

// nilStream is a stream that does not send messages
type nilStream[T any] struct{}

func (s *nilStream[T]) Send(out Result[T]) {
}

func (s *nilStream[T]) Result(value T, err error) {
}

func (s *nilStream[T]) Value(value T) {
}

func (s *nilStream[T]) Error(err error) {
}

func (s *nilStream[T]) Close() {
}

// NewEncodingStream returns a new encoding stream
func NewEncodingStream[T, U any](stream WriteStream[U], encoder func(T, error) (U, error)) WriteStream[T] {
	return &transcodingStream[T, U]{
		stream:     stream,
		transcoder: encoder,
	}
}

// NewDecodingStream returns a new decoding stream
func NewDecodingStream[T, U any](stream WriteStream[T], encoder func(U, error) (T, error)) WriteStream[U] {
	return &transcodingStream[U, T]{
		stream:     stream,
		transcoder: encoder,
	}
}

// transcodingStream is a stream that encodes output
type transcodingStream[T, U any] struct {
	stream     WriteStream[U]
	transcoder func(T, error) (U, error)
}

func (s *transcodingStream[T, U]) Send(result Result[T]) {
	if result.Failed() {
		s.stream.Send(Result[U]{
			Error: result.Error,
		})
	} else {
		s.Value(result.Value)
	}
}

func (s *transcodingStream[T, U]) Result(value T, err error) {
	u, err := s.transcoder(value, err)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(u)
	}
}

func (s *transcodingStream[T, U]) Value(value T) {
	u, err := s.transcoder(value, nil)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(u)
	}
}

func (s *transcodingStream[T, U]) Error(err error) {
	u, err := s.transcoder(nil, err)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(u)
	}
}

func (s *transcodingStream[T, U]) Close() {
	s.stream.Close()
}

// NewCloserStream returns a new stream that runs a function on close
func NewCloserStream[T any](stream WriteStream[T], f func(WriteStream[T])) WriteStream[T] {
	return &closerStream[T]{
		stream: stream,
		closer: f,
	}
}

// closerStream is a stream that runs a function on close
type closerStream[T any] struct {
	stream WriteStream[T]
	closer func(WriteStream[T])
	closed bool
	mu     sync.RWMutex
}

func (s *closerStream[T]) Send(result Result[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.closed {
		s.stream.Send(result)
	}
}

func (s *closerStream[T]) Result(value T, err error) {
	s.Send(Result[T]{
		Value: value,
		Error: err,
	})
}

func (s *closerStream[T]) Value(value T) {
	s.Result(value, nil)
}

func (s *closerStream[T]) Error(err error) {
	s.Result(nil, err)
}

func (s *closerStream[T]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closer(s)
		s.stream.Close()
		s.closed = true
	}
}

// Result is a stream result
type Result[T any] struct {
	Value T
	Error error
}

// Failed returns a boolean indicating whether the operation failed
func (r Result[T]) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Result[T]) Succeeded() bool {
	return !r.Failed()
}
