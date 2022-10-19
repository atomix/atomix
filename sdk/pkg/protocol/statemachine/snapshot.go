// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/gogo/protobuf/proto"
	"io"
)

type Recoverable interface {
	Snapshot(writer *SnapshotWriter) error
	Recover(reader *SnapshotReader) error
}

func NewSnapshotWriter(writer io.Writer) *SnapshotWriter {
	return &SnapshotWriter{
		Writer: writer,
	}
}

type SnapshotWriter struct {
	io.Writer
}

// WriteBool writes a boolean to the given writer
func (w *SnapshotWriter) WriteBool(b bool) error {
	if b {
		if _, err := w.Write([]byte{1}); err != nil {
			return err
		}
	} else {
		if _, err := w.Write([]byte{0}); err != nil {
			return err
		}
	}
	return nil
}

// WriteVarUint64 writes an unsigned variable length integer to the given writer
func (w *SnapshotWriter) WriteVarUint64(x uint64) error {
	for x >= 0x80 {
		if _, err := w.Write([]byte{byte(x) | 0x80}); err != nil {
			return err
		}
		x >>= 7
	}
	if _, err := w.Write([]byte{byte(x)}); err != nil {
		return err
	}
	return nil
}

// WriteVarInt64 writes a signed variable length integer to the given writer
func (w *SnapshotWriter) WriteVarInt64(x int64) error {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return w.WriteVarUint64(ux)
}

// WriteVarInt32 writes a signed 32-bit integer to the given writer
func (w *SnapshotWriter) WriteVarInt32(i int32) error {
	return w.WriteVarInt64(int64(i))
}

// WriteVarInt writes a signed integer to the given writer
func (w *SnapshotWriter) WriteVarInt(i int) error {
	return w.WriteVarInt64(int64(i))
}

// WriteVarUint32 writes an unsigned 32-bit integer to the given writer
func (w *SnapshotWriter) WriteVarUint32(i uint32) error {
	return w.WriteVarUint64(uint64(i))
}

// WriteVarUint writes an unsigned integer to the given writer
func (w *SnapshotWriter) WriteVarUint(i uint) error {
	return w.WriteVarUint64(uint64(i))
}

// WriteString writes a string to the given writer
func (w *SnapshotWriter) WriteString(s string) error {
	return w.WriteBytes([]byte(s))
}

// WriteBytes writes a byte slice to the given writer
func (w *SnapshotWriter) WriteBytes(bytes []byte) error {
	if err := w.WriteVarInt(len(bytes)); err != nil {
		return err
	}
	if _, err := w.Write(bytes); err != nil {
		return err
	}
	return nil
}

func (w *SnapshotWriter) WriteMessage(message proto.Message) error {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return w.WriteBytes(bytes)
}

func NewSnapshotReader(reader io.Reader) *SnapshotReader {
	return &SnapshotReader{
		Reader: reader,
	}
}

type SnapshotReader struct {
	io.Reader
}

// ReadBool reads a boolean from the given reader
func (r *SnapshotReader) ReadBool() (bool, error) {
	bytes := make([]byte, 1)
	if _, err := r.Read(bytes); err != nil {
		return false, err
	}
	return bytes[0] == 1, nil
}

// ReadVarUint64 reads an unsigned variable length integer from the given reader
func (r *SnapshotReader) ReadVarUint64() (uint64, error) {
	var x uint64
	var s uint
	bytes := make([]byte, 1)
	for i := 0; i <= 9; i++ {
		if n, err := r.Read(bytes); err != nil || n == -1 {
			return 0, err
		}
		b := bytes[0]
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, nil
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, nil
}

// ReadVarInt64 reads a signed variable length integer from the given reader
func (r *SnapshotReader) ReadVarInt64() (int64, error) {
	ux, n := r.ReadVarUint64()
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n
}

// ReadVarInt32 reads a signed 32-bit integer from the given reader
func (r *SnapshotReader) ReadVarInt32() (int32, error) {
	i, err := r.ReadVarInt64()
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

// ReadVarInt reads a signed integer from the given reader
func (r *SnapshotReader) ReadVarInt() (int, error) {
	i, err := r.ReadVarInt64()
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

// ReadVarUint32 reads an unsigned 32-bit integer from the given reader
func (r *SnapshotReader) ReadVarUint32() (uint32, error) {
	i, err := r.ReadVarUint64()
	if err != nil {
		return 0, err
	}
	return uint32(i), nil
}

// ReadVarUint reads an unsigned integer from the given reader
func (r *SnapshotReader) ReadVarUint() (uint, error) {
	i, err := r.ReadVarUint64()
	if err != nil {
		return 0, err
	}
	return uint(i), nil
}

// ReadString reads a string from the given reader
func (r *SnapshotReader) ReadString() (string, error) {
	s, err := r.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(s), nil
}

// ReadBytes reads a byte slice from the given reader
func (r *SnapshotReader) ReadBytes() ([]byte, error) {
	length, err := r.ReadVarInt()
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	bytes := make([]byte, length)
	if _, err := r.Read(bytes); err != nil {
		return nil, err
	}
	return bytes, nil
}

func (r *SnapshotReader) ReadMessage(message proto.Message) error {
	bytes, err := r.ReadBytes()
	if err != nil {
		return err
	}
	return proto.Unmarshal(bytes, message)
}
