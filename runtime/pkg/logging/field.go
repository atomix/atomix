// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"fmt"
	"go.uber.org/zap"
	"time"
)

// Field is a structured logger field
type Field interface {
	getZapField() zap.Field
}

type zapField struct {
	field zap.Field
}

// Name returns the field name
func (f *zapField) Name() string {
	return f.field.Key
}

func (f *zapField) getZapField() zap.Field {
	return f.field
}

// Error creates a named field for an error
func Error(name string, err error) Field {
	return &zapField{
		field: zap.Error(err),
	}
}

// Stringer creates a named field for a type implementing Stringer
func Stringer(name string, value fmt.Stringer) Field {
	return &zapField{
		field: zap.Stringer(name, value),
	}
}

// String creates a named string field
func String(name string, value string) Field {
	return &zapField{
		field: zap.String(name, value),
	}
}

// Stringp creates a named string pointer field
func Stringp(name string, value *string) Field {
	return &zapField{
		field: zap.Stringp(name, value),
	}
}

// Strings creates a named string slice field
func Strings(name string, value []string) Field {
	return &zapField{
		field: zap.Strings(name, value),
	}
}

// Int creates a named int field
func Int(name string, value int) Field {
	return &zapField{
		field: zap.Int(name, value),
	}
}

// Intp creates a named int pointer field
func Intp(name string, value *int) Field {
	return &zapField{
		field: zap.Intp(name, value),
	}
}

// Ints creates a named int slice field
func Ints(name string, value []int) Field {
	return &zapField{
		field: zap.Ints(name, value),
	}
}

// Int32 creates a named int32 field
func Int32(name string, value int32) Field {
	return &zapField{
		field: zap.Int32(name, value),
	}
}

// Int32p creates a named int32 pointer field
func Int32p(name string, value *int32) Field {
	return &zapField{
		field: zap.Int32p(name, value),
	}
}

// Int32s creates a named int32 slice field
func Int32s(name string, value []int32) Field {
	return &zapField{
		field: zap.Int32s(name, value),
	}
}

// Int64 creates a named int64 field
func Int64(name string, value int64) Field {
	return &zapField{
		field: zap.Int64(name, value),
	}
}

// Int64p creates a named int64 pointer field
func Int64p(name string, value *int64) Field {
	return &zapField{
		field: zap.Int64p(name, value),
	}
}

// Int64s creates a named int64 slice field
func Int64s(name string, value []int64) Field {
	return &zapField{
		field: zap.Int64s(name, value),
	}
}

// Uint creates a named uint field
func Uint(name string, value uint) Field {
	return &zapField{
		field: zap.Uint(name, value),
	}
}

// Uintp creates a named uint pointer field
func Uintp(name string, value *uint) Field {
	return &zapField{
		field: zap.Uintp(name, value),
	}
}

// Uints creates a named uint slice field
func Uints(name string, value []uint) Field {
	return &zapField{
		field: zap.Uints(name, value),
	}
}

// Uint32 creates a named uint32 field
func Uint32(name string, value uint32) Field {
	return &zapField{
		field: zap.Uint32(name, value),
	}
}

// Uint32p creates a named uint32 pointer field
func Uint32p(name string, value *uint32) Field {
	return &zapField{
		field: zap.Uint32p(name, value),
	}
}

// Uint32s creates a named uint32 slice field
func Uint32s(name string, value []uint32) Field {
	return &zapField{
		field: zap.Uint32s(name, value),
	}
}

// Uint64 creates a named uint64 field
func Uint64(name string, value uint64) Field {
	return &zapField{
		field: zap.Uint64(name, value),
	}
}

// Uint64p creates a named uint64 pointer field
func Uint64p(name string, value *uint64) Field {
	return &zapField{
		field: zap.Uint64p(name, value),
	}
}

// Uint64s creates a named uint64 slice field
func Uint64s(name string, value []uint64) Field {
	return &zapField{
		field: zap.Uint64s(name, value),
	}
}

// Float32 creates a named float32 field
func Float32(name string, value float32) Field {
	return &zapField{
		field: zap.Float32(name, value),
	}
}

// Float32p creates a named float32 pointer field
func Float32p(name string, value *float32) Field {
	return &zapField{
		field: zap.Float32p(name, value),
	}
}

// Float32s creates a named float32 slice field
func Float32s(name string, value []float32) Field {
	return &zapField{
		field: zap.Float32s(name, value),
	}
}

// Float64 creates a named float64 field
func Float64(name string, value float64) Field {
	return &zapField{
		field: zap.Float64(name, value),
	}
}

// Float64p creates a named float64 pointer field
func Float64p(name string, value *float64) Field {
	return &zapField{
		field: zap.Float64p(name, value),
	}
}

// Float64s creates a named float64 slice field
func Float64s(name string, value []float64) Field {
	return &zapField{
		field: zap.Float64s(name, value),
	}
}

// Complex64 creates a named complex64 field
func Complex64(name string, value complex64) Field {
	return &zapField{
		field: zap.Complex64(name, value),
	}
}

// Complex64p creates a named complex64 pointer field
func Complex64p(name string, value *complex64) Field {
	return &zapField{
		field: zap.Complex64p(name, value),
	}
}

// Complex64s creates a named complex64 slice field
func Complex64s(name string, value []complex64) Field {
	return &zapField{
		field: zap.Complex64s(name, value),
	}
}

// Complex128 creates a named complex128 field
func Complex128(name string, value complex128) Field {
	return &zapField{
		field: zap.Complex128(name, value),
	}
}

// Complex128p creates a named complex128 pointer field
func Complex128p(name string, value *complex128) Field {
	return &zapField{
		field: zap.Complex128p(name, value),
	}
}

// Complex128s creates a named complex128 slice field
func Complex128s(name string, value []complex128) Field {
	return &zapField{
		field: zap.Complex128s(name, value),
	}
}

// Bool creates a named bool field
func Bool(name string, value bool) Field {
	return &zapField{
		field: zap.Bool(name, value),
	}
}

// Boolp creates a named bool pointer field
func Boolp(name string, value *bool) Field {
	return &zapField{
		field: zap.Boolp(name, value),
	}
}

// Bools creates a named bool slice field
func Bools(name string, value []bool) Field {
	return &zapField{
		field: zap.Bools(name, value),
	}
}

// Time creates a named Time field
func Time(name string, value time.Time) Field {
	return &zapField{
		field: zap.Time(name, value),
	}
}

// Timep creates a named Time pointer field
func Timep(name string, value *time.Time) Field {
	return &zapField{
		field: zap.Timep(name, value),
	}
}

// Times creates a named Time slice field
func Times(name string, value []time.Time) Field {
	return &zapField{
		field: zap.Times(name, value),
	}
}

// Duration creates a named Duration field
func Duration(name string, value time.Duration) Field {
	return &zapField{
		field: zap.Duration(name, value),
	}
}

// Durationp creates a named Duration pointer field
func Durationp(name string, value *time.Duration) Field {
	return &zapField{
		field: zap.Durationp(name, value),
	}
}

// Durations creates a named Duration slice field
func Durations(name string, value []time.Duration) Field {
	return &zapField{
		field: zap.Durations(name, value),
	}
}

// Byte creates a named byte field
func Byte(name string, value byte) Field {
	return &zapField{
		field: zap.Binary(name, []byte{value}),
	}
}

// Bytes creates a named byte slice field
func Bytes(name string, value []byte) Field {
	return &zapField{
		field: zap.Binary(name, value),
	}
}

// ByteString creates a named byte string field
func ByteString(name string, value []byte) Field {
	return &zapField{
		field: zap.ByteString(name, value),
	}
}

// ByteStrings creates a named byte string slice field
func ByteStrings(name string, value [][]byte) Field {
	return &zapField{
		field: zap.ByteStrings(name, value),
	}
}
