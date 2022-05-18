// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package meta

import (
	runtimev1 "github.com/atomix/runtime/api/atomix/runtime/v1"
	"github.com/atomix/runtime/pkg/time"
)

func Equal(m1, m2 runtimev1.ObjectMeta) bool {
	return FromProto(m1).Equal(FromProto(m2))
}

// NewRevision creates a new object metadata with the given revision
func NewRevision(revision Revision) ObjectMeta {
	return ObjectMeta{
		Revision: revision,
	}
}

// NewTimestamped creates a new object metadata with the given timestamp
func NewTimestamped(timestamp time.Timestamp) ObjectMeta {
	return ObjectMeta{
		Timestamp: timestamp,
	}
}

// FromProto creates new object metadata from the given proto metadata
func FromProto(meta runtimev1.ObjectMeta) ObjectMeta {
	var revision Revision
	if meta.Revision != 0 {
		revision = Revision(meta.Revision)
	}
	var timestamp time.Timestamp
	if meta.Timestamp != nil {
		timestamp = time.NewTimestamp(*meta.Timestamp)
	}
	return ObjectMeta{
		Revision:  revision,
		Timestamp: timestamp,
		Tombstone: meta.Type == runtimev1.ObjectMeta_TOMBSTONE,
	}
}

// Object is an interface for objects
type Object interface {
	// Meta returns the object metadata
	Meta() ObjectMeta
}

// ObjectMeta contains metadata about an object
type ObjectMeta struct {
	Revision  Revision
	Timestamp time.Timestamp
	Tombstone bool
}

// AsObject returns the metadata as a non-tombstone
func (m ObjectMeta) AsObject() ObjectMeta {
	copy := m
	copy.Tombstone = false
	return copy
}

// AsTombstone returns the metadata as a tombstone
func (m ObjectMeta) AsTombstone() ObjectMeta {
	copy := m
	copy.Tombstone = true
	return copy
}

// Meta implements the Object interface
func (m ObjectMeta) Meta() ObjectMeta {
	return m
}

// Proto returns the metadata in Protobuf format
func (m ObjectMeta) Proto() runtimev1.ObjectMeta {
	meta := runtimev1.ObjectMeta{}
	if m.Revision > 0 {
		meta.Revision = runtimev1.Revision(m.Revision)
	}
	if m.Timestamp != nil {
		timestamp := m.Timestamp.Scheme().Codec().EncodeTimestamp(m.Timestamp)
		meta.Timestamp = &timestamp
	}
	if m.Tombstone {
		meta.Type = runtimev1.ObjectMeta_TOMBSTONE
	} else {
		meta.Type = runtimev1.ObjectMeta_OBJECT
	}
	return meta
}

func (m ObjectMeta) Equal(meta ObjectMeta) bool {
	if m.Revision != meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.Equal(meta.Timestamp) {
		return false
	}
	return true
}

func (m ObjectMeta) Before(meta ObjectMeta) bool {
	if m.Revision != 0 && meta.Revision != 0 && m.Revision >= meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.Before(meta.Timestamp) {
		return false
	}
	return true
}

func (m ObjectMeta) After(meta ObjectMeta) bool {
	if m.Revision != 0 && meta.Revision != 0 && m.Revision <= meta.Revision {
		return false
	}
	if m.Timestamp != nil && meta.Timestamp != nil && !m.Timestamp.After(meta.Timestamp) {
		return false
	}
	return true
}

// Revision is a revision number
type Revision uint64
