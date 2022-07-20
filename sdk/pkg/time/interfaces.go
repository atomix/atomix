// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package time

import runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"

// NewTimestamp creates new object timestamp from the given proto timestamp
func NewTimestamp(meta runtimev1.Timestamp) Timestamp {
	switch t := meta.Timestamp.(type) {
	case *runtimev1.Timestamp_PhysicalTimestamp:
		return NewPhysicalTimestamp(PhysicalTime(t.PhysicalTimestamp.Time))
	case *runtimev1.Timestamp_LogicalTimestamp:
		return NewLogicalTimestamp(LogicalTime(t.LogicalTimestamp.Time))
	case *runtimev1.Timestamp_EpochTimestamp:
		return NewEpochTimestamp(Epoch(t.EpochTimestamp.Epoch), LogicalTime(t.EpochTimestamp.Time))
	case *runtimev1.Timestamp_CompositeTimestamp:
		timestamps := make([]Timestamp, 0, len(t.CompositeTimestamp.Timestamps))
		for _, timestamp := range t.CompositeTimestamp.Timestamps {
			timestamps = append(timestamps, NewTimestamp(timestamp))
		}
		return NewCompositeTimestamp(timestamps...)
	default:
		panic("unknown timestamp type")
	}
}

// Scheme it a time scheme
type Scheme interface {
	// Name returns the scheme's name
	Name() string

	// Codec returns the scheme's codec
	Codec() Codec

	// NewClock creates a new clock
	NewClock() Clock
}

// Clock is an interface for clocks
type Clock interface {
	// Scheme returns the clock's scheme
	Scheme() Scheme
	// Get gets the current timestamp
	Get() Timestamp
	// Increment increments the clock
	Increment() Timestamp
	// Update updates the timestamp
	Update(Timestamp) Timestamp
}

// Codec is a time codec
type Codec interface {
	EncodeTimestamp(Timestamp) runtimev1.Timestamp
	DecodeTimestamp(runtimev1.Timestamp) (Timestamp, error)
}

// Timestamp is a timestamp
type Timestamp interface {
	Scheme() Scheme
	Before(Timestamp) bool
	After(Timestamp) bool
	Equal(Timestamp) bool
}
