// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// Enabled indicates whether the log level is enabled
func (l Level) Enabled(level Level) bool {
	return l <= level
}

// String :
func (l Level) String() string {
	return [...]string{"", "debug", "info", "warn", "error", "fatal", "panic"}[l]
}

func levelToAtomicLevel(l Level) zap.AtomicLevel {
	switch l {
	case DebugLevel:
		return zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case InfoLevel:
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case WarnLevel:
		return zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case ErrorLevel:
		return zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	case FatalLevel:
		return zap.NewAtomicLevelAt(zapcore.FatalLevel)
	case PanicLevel:
		return zap.NewAtomicLevelAt(zapcore.PanicLevel)
	default:
		return zap.NewAtomicLevelAt(zapcore.FatalLevel)
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
