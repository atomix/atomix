// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"go.uber.org/zap"
)

// Output is a logging output
type Output interface {
	Name() string
	Level() Level
	getChild(name string) Output
	WithLevel(level Level) Output
	WithFields(fields ...Field) Output
	WithSkipCalls(calls int) Output
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	Panic(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Sync() error
}

type OutputOption func(*OutputConfig)

func WithLevel(level Level) OutputOption {
	levelString := level.String()
	return func(config *OutputConfig) {
		config.Level = &levelString
	}
}

// NewOutput creates a new logger output
func NewOutput(sink Sink, opts ...OutputOption) Output {
	var config OutputConfig
	for _, opt := range opts {
		opt(&config)
	}
	return newOutput("", sink, config)
}

func newOutput(name string, sink Sink, config OutputConfig) Output {
	return &zapOutput{
		name:  name,
		sink:  sink,
		level: config.GetLevel(),
	}
}

// zapOutput is a logging output implementation
type zapOutput struct {
	name  string
	sink  Sink
	level Level
}

func (o *zapOutput) Name() string {
	return o.name
}

func (o *zapOutput) Level() Level {
	return o.level
}

func (o *zapOutput) getChild(name string) Output {
	return &zapOutput{
		name:  o.name,
		sink:  o.sink.getChild(name),
		level: o.level,
	}
}

func (o *zapOutput) WithLevel(level Level) Output {
	return &zapOutput{
		name:  o.name,
		sink:  o.sink,
		level: level,
	}
}

func (o *zapOutput) WithFields(fields ...Field) Output {
	return &zapOutput{
		name:  o.name,
		sink:  o.sink.WithFields(fields...),
		level: o.level,
	}
}

func (o *zapOutput) WithSkipCalls(calls int) Output {
	return &zapOutput{
		name:  o.name,
		sink:  o.sink.WithSkipCalls(calls),
		level: o.level,
	}
}

func (o *zapOutput) getZapFields(fields ...Field) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zapFields := make([]zap.Field, len(fields))
	for i, field := range fields {
		zapFields[i] = field.getZapField()
	}
	return zapFields
}

func (o *zapOutput) Debug(msg string, fields ...Field) {
	if o.level.Enabled(DebugLevel) {
		o.sink.Debug(msg, fields...)
	}
}

func (o *zapOutput) Info(msg string, fields ...Field) {
	if o.level.Enabled(InfoLevel) {
		o.sink.Info(msg, fields...)
	}
}

func (o *zapOutput) Error(msg string, fields ...Field) {
	if o.level.Enabled(ErrorLevel) {
		o.sink.Error(msg, fields...)
	}
}

func (o *zapOutput) Fatal(msg string, fields ...Field) {
	if o.level.Enabled(FatalLevel) {
		o.sink.Fatal(msg, fields...)
	}
}

func (o *zapOutput) Panic(msg string, fields ...Field) {
	if o.level.Enabled(PanicLevel) {
		o.sink.Panic(msg, fields...)
	}
}

func (o *zapOutput) Warn(msg string, fields ...Field) {
	if o.level.Enabled(WarnLevel) {
		o.sink.Warn(msg, fields...)
	}
}

func (o *zapOutput) Sync() error {
	return o.sink.Sync()
}

var _ Output = &zapOutput{}
