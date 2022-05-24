// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package time

import (
	metav1 "github.com/atomix/runtime/api/atomix/primitive/meta/v1"
	"sync"
)

const compositeSchemeName = "Composite"

// newCompositeScheme creates a new composite scheme
func newCompositeScheme(schemes ...Scheme) Scheme {
	codecs := make([]Codec, len(schemes))
	for i, scheme := range schemes {
		codecs[i] = scheme.Codec()
	}
	return compositeScheme{
		schemes: schemes,
		codec:   CompositeTimestampCodec{codecs},
	}
}

type compositeScheme struct {
	schemes []Scheme
	codec   Codec
}

func (s compositeScheme) Name() string {
	return compositeSchemeName
}

func (s compositeScheme) Codec() Codec {
	return s.codec
}

func (s compositeScheme) NewClock() Clock {
	return NewCompositeClock(s.schemes...)
}

// NewCompositeClock creates a new composite clock
func NewCompositeClock(schemes ...Scheme) Clock {
	scheme := newCompositeScheme(schemes...)
	clocks := make([]Clock, len(schemes))
	timestamps := make([]Timestamp, len(schemes))
	for i, scheme := range schemes {
		clock := scheme.NewClock()
		clocks[i] = clock
		timestamps[i] = clock.Get()
	}
	return &CompositeClock{
		scheme:    scheme,
		clocks:    clocks,
		timestamp: NewCompositeTimestamp(timestamps...),
	}
}

// CompositeClock is a clock that produces CompositeTimestamps
type CompositeClock struct {
	scheme    Scheme
	clocks    []Clock
	timestamp Timestamp
	mu        sync.RWMutex
}

func (c *CompositeClock) Scheme() Scheme {
	return c.scheme
}

func (c *CompositeClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *CompositeClock) Increment() Timestamp {
	timestamps := make([]Timestamp, len(c.clocks))
	for i, clock := range c.clocks {
		timestamps[i] = clock.Increment()
	}
	return NewCompositeTimestamp(timestamps...)
}

func (c *CompositeClock) Update(t Timestamp) Timestamp {
	update, ok := t.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}

	timestamps := make([]Timestamp, len(c.clocks))
	for i, clock := range c.clocks {
		timestamps[i] = clock.Update(update.Timestamps[i])
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewCompositeTimestamp(timestamps...)
	return c.timestamp
}

// NewCompositeTimestamp creates a new composite Timestamp from the given set of Timestamps
func NewCompositeTimestamp(timestamps ...Timestamp) Timestamp {
	schemes := make([]Scheme, len(timestamps))
	for i, timestamp := range timestamps {
		schemes[i] = timestamp.Scheme()
	}
	return CompositeTimestamp{
		scheme:     newCompositeScheme(schemes...),
		Timestamps: timestamps,
	}
}

// CompositeTimestamp is a composite Timestamp implementation
type CompositeTimestamp struct {
	scheme     Scheme
	Timestamps []Timestamp
}

func (t CompositeTimestamp) Scheme() Scheme {
	return t.scheme
}

func (t CompositeTimestamp) Before(u Timestamp) bool {
	v, ok := u.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}
	if len(t.Timestamps) != len(v.Timestamps) {
		panic("incompatible composite timestamps")
	}
	for i := 0; i < len(t.Timestamps); i++ {
		t1 := t.Timestamps[i]
		t2 := v.Timestamps[i]
		if t1.Before(t2) {
			return true
		} else if i > 0 {
			for j := 0; j < i; j++ {
				v1 := t.Timestamps[j]
				v2 := v.Timestamps[j]
				if !v1.Equal(v2) {
					return false
				}
			}
			if !t1.Before(t2) {
				return false
			}
		}
	}
	return true
}

func (t CompositeTimestamp) After(u Timestamp) bool {
	v, ok := u.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}
	if len(t.Timestamps) != len(v.Timestamps) {
		panic("incompatible composite timestamps")
	}
	for i := 0; i < len(t.Timestamps); i++ {
		t1 := t.Timestamps[i]
		t2 := v.Timestamps[i]
		if t1.After(t2) {
			return true
		} else if i > 0 {
			for j := 0; j < i; j++ {
				v1 := t.Timestamps[j]
				v2 := v.Timestamps[j]
				if !v1.Equal(v2) {
					return false
				}
			}
			if !t1.After(t2) {
				return false
			}
		}
	}
	return true
}

func (t CompositeTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(CompositeTimestamp)
	if !ok {
		panic("not a composite timestamp")
	}
	if len(t.Timestamps) != len(v.Timestamps) {
		panic("incompatible composite timestamps")
	}
	for i := 0; i < len(t.Timestamps); i++ {
		t1 := t.Timestamps[i]
		t2 := v.Timestamps[i]
		if !t1.Equal(t2) {
			return false
		}
	}
	return true
}

// CompositeTimestampCodec is a codec for Composite timestamps
type CompositeTimestampCodec struct {
	codecs []Codec
}

func (c CompositeTimestampCodec) EncodeTimestamp(timestamp Timestamp) metav1.Timestamp {
	t, ok := timestamp.(CompositeTimestamp)
	if !ok {
		panic("expected CompositeTimestamp")
	}
	timestamps := make([]metav1.Timestamp, 0, len(t.Timestamps))
	for _, timestamp := range t.Timestamps {
		timestamps = append(timestamps, timestamp.Scheme().Codec().EncodeTimestamp(timestamp))
	}
	return metav1.Timestamp{
		Timestamp: &metav1.Timestamp_CompositeTimestamp{
			CompositeTimestamp: &metav1.CompositeTimestamp{
				Timestamps: timestamps,
			},
		},
	}
}

func (c CompositeTimestampCodec) DecodeTimestamp(timestamp metav1.Timestamp) (Timestamp, error) {
	timestamps := make([]Timestamp, 0, len(timestamp.GetCompositeTimestamp().Timestamps))
	for _, timestamp := range timestamp.GetCompositeTimestamp().Timestamps {
		timestamps = append(timestamps, NewTimestamp(timestamp))
	}
	return NewCompositeTimestamp(timestamps...), nil
}
