// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	zp "go.uber.org/zap"
	zc "go.uber.org/zap/zapcore"
	"strings"
)

// Level :
type Level int32

const (
	// EmptyLevel :
	EmptyLevel Level = iota
	// DebugLevel logs a message at debug level
	DebugLevel
	// InfoLevel logs a message at info level
	InfoLevel
	// WarnLevel logs a message at warning level
	WarnLevel
	// ErrorLevel logs a message at error level
	ErrorLevel
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel
	// PanicLevel logs a message, then panics.
	PanicLevel
)

// String :
func (l Level) String() string {
	return [...]string{"", "debug", "info", "warn", "error", "fatal", "panic"}[l]
}

func levelToAtomicLevel(l Level) zp.AtomicLevel {
	switch l {
	case DebugLevel:
		return zp.NewAtomicLevelAt(zc.DebugLevel)
	case InfoLevel:
		return zp.NewAtomicLevelAt(zc.InfoLevel)
	case WarnLevel:
		return zp.NewAtomicLevelAt(zc.WarnLevel)
	case ErrorLevel:
		return zp.NewAtomicLevelAt(zc.ErrorLevel)
	case FatalLevel:
		return zp.NewAtomicLevelAt(zc.FatalLevel)
	case PanicLevel:
		return zp.NewAtomicLevelAt(zc.PanicLevel)
	default:
		return zp.NewAtomicLevelAt(zc.FatalLevel)
	}
}

func levelStringToLevel(l string) Level {
	switch strings.ToLower(l) {
	case DebugLevel.String():
		return DebugLevel
	case InfoLevel.String():
		return InfoLevel
	case WarnLevel.String():
		return WarnLevel
	case ErrorLevel.String():
		return ErrorLevel
	case FatalLevel.String():
		return FatalLevel
	case PanicLevel.String():
		return PanicLevel
	default:
		return EmptyLevel
	}
}
