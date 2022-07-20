// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package time

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"sync"
)

const epochSchemeName = "Epoch"

var EpochScheme = newEpochScheme()

// newEpochScheme creates a new Epoch scheme
func newEpochScheme() Scheme {
	return epochScheme{
		codec: EpochTimestampCodec{},
	}
}

type epochScheme struct {
	codec Codec
}

func (s epochScheme) Name() string {
	return epochSchemeName
}

func (s epochScheme) Codec() Codec {
	return s.codec
}

func (s epochScheme) NewClock() Clock {
	return NewEpochClock()
}

// NewEpochClock creates a new epoch clock
func NewEpochClock() Clock {
	return &EpochClock{
		timestamp: NewEpochTimestamp(Epoch(0), LogicalTime(0)).(EpochTimestamp),
	}
}

// EpochClock is a clock that produces EpochTimestamps
type EpochClock struct {
	timestamp EpochTimestamp
	mu        sync.RWMutex
}

func (c *EpochClock) Scheme() Scheme {
	return EpochScheme
}

func (c *EpochClock) Get() Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

func (c *EpochClock) Increment() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = NewEpochTimestamp(c.timestamp.Epoch, c.timestamp.Time+1).(EpochTimestamp)
	return c.timestamp
}

func (c *EpochClock) Update(t Timestamp) Timestamp {
	update, ok := t.(EpochTimestamp)
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

type Epoch uint64

// NewEpochTimestamp creates a new Epoch based Timestamp
func NewEpochTimestamp(epoch Epoch, time LogicalTime) Timestamp {
	return EpochTimestamp{
		Epoch: epoch,
		Time:  time,
	}
}

// EpochTimestamp is a Timestamp based on an Epoch and LogicalTime
type EpochTimestamp struct {
	Epoch Epoch
	Time  LogicalTime
}

func (t EpochTimestamp) Scheme() Scheme {
	return EpochScheme
}

func (t EpochTimestamp) Before(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch < v.Epoch || (t.Epoch == v.Epoch && t.Time < v.Time)
}

func (t EpochTimestamp) After(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch > v.Epoch || (t.Epoch == v.Epoch && t.Time > v.Time)
}

func (t EpochTimestamp) Equal(u Timestamp) bool {
	v, ok := u.(EpochTimestamp)
	if !ok {
		panic("not an epoch timestamp")
	}
	return t.Epoch == v.Epoch && t.Time == v.Time
}

// EpochTimestampCodec is a codec for epoch timestamps
type EpochTimestampCodec struct{}

func (c EpochTimestampCodec) EncodeTimestamp(timestamp Timestamp) runtimev1.Timestamp {
	t, ok := timestamp.(EpochTimestamp)
	if !ok {
		panic("expected EpochTimestamp")
	}
	return runtimev1.Timestamp{
		Timestamp: &runtimev1.Timestamp_EpochTimestamp{
			EpochTimestamp: &runtimev1.EpochTimestamp{
				Epoch: runtimev1.Epoch(t.Epoch),
				Time:  runtimev1.LogicalTime(t.Time),
			},
		},
	}
}

func (c EpochTimestampCodec) DecodeTimestamp(timestamp runtimev1.Timestamp) (Timestamp, error) {
	return NewEpochTimestamp(Epoch(timestamp.GetEpochTimestamp().Epoch), LogicalTime(timestamp.GetEpochTimestamp().Time)), nil
}
