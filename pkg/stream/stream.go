// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"container/list"
	"sync"
)

// ReadStream is a state machine read stream
type ReadStream interface {
	// Receive receives the next result
	Receive() (Result, bool)

	// Drain drains the stream
	Drain()
}

// WriteStream is a state machine write stream
type WriteStream interface {
	// Send sends an output on the stream
	Send(out Result)

	// Result sends a result on the stream
	Result(value interface{}, err error)

	// Value sends a value on the stream
	Value(value interface{})

	// Error sends an error on the stream
	Error(err error)

	// Close closes the stream
	Close()
}

// Stream is a read/write stream
type Stream interface {
	ReadStream
	WriteStream
}

// NewUnaryStream returns a new read/write stream that expects one result
func NewUnaryStream() Stream {
	return &unaryStream{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

// unaryStream is a stream that expects one result
type unaryStream struct {
	result *Result
	closed bool
	cond   *sync.Cond
}

func (s *unaryStream) Receive() (Result, bool) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.closed {
		return Result{}, false
	}
	if s.result == nil {
		if s.closed {
			return Result{}, false
		}
		s.cond.Wait()
	}
	result := s.result
	s.result = nil
	return *result, true
}

func (s *unaryStream) Drain() {
	s.Close()
}

func (s *unaryStream) Send(result Result) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if !s.closed {
		s.result = &result
		s.closed = true
	}
	s.cond.Signal()
}

func (s *unaryStream) Result(value interface{}, err error) {
	s.Send(Result{
		Value: value,
		Error: err,
	})
}

func (s *unaryStream) Value(value interface{}) {
	s.Send(Result{
		Value: value,
	})
}

func (s *unaryStream) Error(err error) {
	s.Send(Result{
		Error: err,
	})
}

func (s *unaryStream) Close() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	s.closed = true
}

// NewBufferedStream returns a new buffered read/write stream
func NewBufferedStream() Stream {
	return &bufferedStream{
		buffer: list.New(),
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

// bufferedStream is a buffered read/write stream
type bufferedStream struct {
	buffer *list.List
	closed bool
	cond   *sync.Cond
}

func (s *bufferedStream) Receive() (Result, bool) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.buffer == nil {
		return Result{}, false
	}
	for s.buffer.Len() == 0 {
		if s.closed {
			return Result{}, false
		}
		s.cond.Wait()
	}
	result := s.buffer.Front().Value.(Result)
	s.buffer.Remove(s.buffer.Front())
	return result, true
}

func (s *bufferedStream) Drain() {
	s.cond.L.Lock()
	defer s.cond.L.Lock()
	s.buffer = nil
}

func (s *bufferedStream) Send(result Result) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.buffer != nil {
		s.buffer.PushBack(result)
		s.cond.Signal()
	}
}

func (s *bufferedStream) Result(value interface{}, err error) {
	s.Send(Result{
		Value: value,
		Error: err,
	})
}

func (s *bufferedStream) Value(value interface{}) {
	s.Send(Result{
		Value: value,
	})
}

func (s *bufferedStream) Error(err error) {
	s.Send(Result{
		Error: err,
	})
}

func (s *bufferedStream) Close() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if !s.closed {
		s.closed = true
		s.cond.Signal()
	}
}

// NewChannelStream returns a new channel-based stream
func NewChannelStream(ch chan Result) Stream {
	return &channelStream{
		ch: ch,
	}
}

// channelStream is a channel-based stream
type channelStream struct {
	ch chan Result
}

func (s *channelStream) Receive() (Result, bool) {
	result, ok := <-s.ch
	return result, ok
}

func (s *channelStream) Drain() {
	go func() {
		for range s.ch {
		}
	}()
}

func (s *channelStream) Send(result Result) {
	s.ch <- result
}

func (s *channelStream) Result(value interface{}, err error) {
	s.Send(Result{
		Value: value,
		Error: err,
	})
}

func (s *channelStream) Value(value interface{}) {
	s.Result(value, nil)
}

func (s *channelStream) Error(err error) {
	s.Result(nil, err)
}

func (s *channelStream) Close() {
	close(s.ch)
}

// NewNilStream returns a disconnected stream
func NewNilStream() WriteStream {
	return &nilStream{}
}

// nilStream is a stream that does not send messages
type nilStream struct{}

func (s *nilStream) Send(out Result) {
}

func (s *nilStream) Result(value interface{}, err error) {
}

func (s *nilStream) Value(value interface{}) {
}

func (s *nilStream) Error(err error) {
}

func (s *nilStream) Close() {
}

// NewEncodingStream returns a new encoding stream
func NewEncodingStream(stream WriteStream, encoder func(interface{}, error) (interface{}, error)) WriteStream {
	return &transcodingStream{
		stream:     stream,
		transcoder: encoder,
	}
}

// NewDecodingStream returns a new decoding stream
func NewDecodingStream(stream WriteStream, encoder func(interface{}, error) (interface{}, error)) WriteStream {
	return &transcodingStream{
		stream:     stream,
		transcoder: encoder,
	}
}

// transcodingStream is a stream that encodes output
type transcodingStream struct {
	stream     WriteStream
	transcoder func(interface{}, error) (interface{}, error)
}

func (s *transcodingStream) Send(result Result) {
	if result.Failed() {
		s.stream.Send(result)
	} else {
		s.Value(result.Value)
	}
}

func (s *transcodingStream) Result(value interface{}, err error) {
	bytes, err := s.transcoder(value, err)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(bytes)
	}
}

func (s *transcodingStream) Value(value interface{}) {
	bytes, err := s.transcoder(value, nil)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(bytes)
	}
}

func (s *transcodingStream) Error(err error) {
	bytes, err := s.transcoder(nil, err)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(bytes)
	}
}

func (s *transcodingStream) Close() {
	s.stream.Close()
}

// NewCloserStream returns a new stream that runs a function on close
func NewCloserStream(stream WriteStream, f func(WriteStream)) WriteStream {
	return &closerStream{
		stream: stream,
		closer: f,
	}
}

// closerStream is a stream that runs a function on close
type closerStream struct {
	stream WriteStream
	closer func(WriteStream)
	closed bool
	mu     sync.RWMutex
}

func (s *closerStream) Send(result Result) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.closed {
		s.stream.Send(result)
	}
}

func (s *closerStream) Result(value interface{}, err error) {
	s.Send(Result{
		Value: value,
		Error: err,
	})
}

func (s *closerStream) Value(value interface{}) {
	s.Result(value, nil)
}

func (s *closerStream) Error(err error) {
	s.Result(nil, err)
}

func (s *closerStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closer(s)
		s.stream.Close()
		s.closed = true
	}
}

// Result is a stream result
type Result struct {
	Value interface{}
	Error error
}

// Failed returns a boolean indicating whether the operation failed
func (r Result) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Result) Succeeded() bool {
	return !r.Failed()
}
