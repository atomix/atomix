// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package time

import (
	metav1 "github.com/atomix/runtime/api/atomix/primitive/meta/v1"
	"sync"
)

const logicalSchemeName = "Logical"

// LogicalScheme is a default Scheme for logical time
var LogicalScheme = newLogicalScheme()

// newLogicalScheme creates a new logical scheme
func newLogicalScheme() Scheme {
	return logicalScheme{
		codec: LogicalTimestampCodec{},
	}
}

type logicalScheme struct {
	codec Codec
}

func (s logicalScheme) Name() string {
	return logicalSchemeName
}

func (s logicalScheme) Codec() Codec {
	return s.codec
}

func (s logicalScheme) NewClock() Clock {
	return NewLogicalClock()
}

// NewLogicalClock creates a new logical clock
func NewLogicalClock() Clock {
	return &LogicalClock{
		timestamp: NewLogicalTimestamp(LogicalTime(0)).(LogicalTimestamp),
	}
}

// LogicalClock is a clock that produces LogicalTimestamps
type LogicalClock struct {
	timestamp LogicalTimestamp
	mu        sync.RWMutex
}

func (c *LogicalClock) Scheme() Scheme {
	return LogicalScheme
}

func (c *LogicalClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *LogicalClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	timestamp := LogicalTimestamp{
		Time: c.timestamp.Time + 1,
	}
	c.timestamp = timestamp
	return timestamp
}

func (c *LogicalClock) Update(t Timestamp) Timestamp {
	update, ok := t.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}

	c.mu.RLock()
	current := c.timestamp
	c.mu.RUnlock()
	if !update.After(current) {
		return current
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if update.After(c.timestamp) {
		c.timestamp = update
	}
	return c.timestamp
}

// LogicalTime is an instant in logical time
type LogicalTime uint64

// NewLogicalTimestamp creates a new logical Timestamp
func NewLogicalTimestamp(time LogicalTime) Timestamp {
	return LogicalTimestamp{
		Time: time,
	}
}

// LogicalTimestamp is a logical timestamp
type LogicalTimestamp struct {
	Time LogicalTime
}

func (t LogicalTimestamp) Scheme() Scheme {
	return LogicalScheme
}

func (t LogicalTimestamp) Increment() LogicalTimestamp {
	return LogicalTimestamp{
		Time: t.Time + 1,
	}
}

func (t LogicalTimestamp) Before(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time < v.Time
}

func (t LogicalTimestamp) After(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time > v.Time
}

func (t LogicalTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(LogicalTimestamp)
	if !ok {
		panic("not a logical timestamp")
	}
	return t.Time == v.Time
}

// LogicalTimestampCodec is a codec for logical timestamps
type LogicalTimestampCodec struct{}

func (c LogicalTimestampCodec) EncodeTimestamp(timestamp Timestamp) metav1.Timestamp {
	t, ok := timestamp.(LogicalTimestamp)
	if !ok {
		panic("expected LogicalTimestamp")
	}
	return metav1.Timestamp{
		Timestamp: &metav1.Timestamp_LogicalTimestamp{
			LogicalTimestamp: &metav1.LogicalTimestamp{
				Time: metav1.LogicalTime(t.Time),
			},
		},
	}
}

func (c LogicalTimestampCodec) DecodeTimestamp(timestamp metav1.Timestamp) (Timestamp, error) {
	return NewLogicalTimestamp(LogicalTime(timestamp.GetLogicalTimestamp().Time)), nil
}
