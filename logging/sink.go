// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"strings"
)

// Sink is a logging sink
type Sink interface {
	getChild(name string) Sink
	WithFields(fields ...Field) Sink
	WithSkipCalls(calls int) Sink
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	Panic(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Sync() error
}

type SinkOption func(*SinkConfig)

func WithEncoding(encoding Encoding) SinkOption {
	return func(config *SinkConfig) {
		config.Encoding = &encoding
	}
}

func WithMessageKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.MessageKey = key
	}
}

func WithLevelKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.LevelKey = key
	}
}

func WithTimeKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.TimeKey = key
	}
}

func WithNameKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.NameKey = key
	}
}

func WithCallerKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.CallerKey = key
	}
}

func WithFunctionKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.FunctionKey = key
	}
}

func WithStacktraceKey(key string) SinkOption {
	return func(config *SinkConfig) {
		config.StacktraceKey = key
	}
}

func WithSkipLineEnding() SinkOption {
	return func(config *SinkConfig) {
		config.SkipLineEnding = true
	}
}

func WithLineEnding(lineEnding string) SinkOption {
	return func(config *SinkConfig) {
		config.LineEnding = lineEnding
	}
}

func WithLevelEncoder(encoder zapcore.LevelEncoder) SinkOption {
	return func(config *SinkConfig) {
		config.EncodeLevel = encoder
	}
}

func WithTimeEncoder(encoder zapcore.TimeEncoder) SinkOption {
	return func(config *SinkConfig) {
		config.EncodeTime = encoder
	}
}

func WithDurationEncoder(encoder zapcore.DurationEncoder) SinkOption {
	return func(config *SinkConfig) {
		config.EncodeDuration = encoder
	}
}

func WithCallerEncoder(encoder zapcore.CallerEncoder) SinkOption {
	return func(config *SinkConfig) {
		config.EncodeCaller = encoder
	}
}

func WithNameEncoder(encoder zapcore.NameEncoder) SinkOption {
	return func(config *SinkConfig) {
		config.EncodeName = encoder
	}
}

func NewSink(writer io.Writer, opts ...SinkOption) (Sink, error) {
	var config SinkConfig
	for _, opt := range opts {
		opt(&config)
	}
	return newZapSink(config, &zapSyncWriter{writer})
}

func newEncoder(config SinkConfig) (zapcore.Encoder, error) {
	switch config.GetEncoding() {
	case ConsoleEncoding:
		return zapcore.NewConsoleEncoder(config.EncoderConfig), nil
	case JSONEncoding:
		return zapcore.NewJSONEncoder(config.EncoderConfig), nil
	default:
		return nil, fmt.Errorf("unknown encoding %s", config.GetEncoding())
	}
}

func newSink(config SinkConfig) (Sink, error) {
	writer, _, err := zap.Open(config.Path)
	if err != nil {
		return nil, err
	}
	return newZapSink(config, writer)
}

func newZapSink(config SinkConfig, writer zapcore.WriteSyncer) (Sink, error) {
	var zapConfig zap.Config
	zapConfig.EncoderConfig = config.EncoderConfig
	zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	zapConfig.Encoding = string(config.GetEncoding())

	encoder, err := newEncoder(config)
	if err != nil {
		return nil, err
	}

	zapLogger, err := zapConfig.Build(zap.AddCallerSkip(4))
	if err != nil {
		return nil, err
	}
	zapLogger = zapLogger.WithOptions(
		zap.WrapCore(
			func(zapcore.Core) zapcore.Core {
				return zapcore.NewCore(encoder, writer, zap.DebugLevel)
			}))
	return &zapSink{
		root:   zapLogger,
		logger: zapLogger,
	}, nil
}

// zapSink is a logging output implementation
type zapSink struct {
	root   *zap.Logger
	path   []string
	logger *zap.Logger
}

func (o *zapSink) getChild(name string) Sink {
	path := o.path
	if name != "" {
		path = append(path, name)
	}
	return &zapSink{
		root:   o.root,
		path:   path,
		logger: o.root.Named(strings.Join(path, pathSep)),
	}
}

func (o *zapSink) WithFields(fields ...Field) Sink {
	return &zapSink{
		root:   o.root,
		path:   o.path,
		logger: o.logger.With(o.getZapFields(fields...)...),
	}
}

func (o *zapSink) WithSkipCalls(calls int) Sink {
	return &zapSink{
		root:   o.root,
		path:   o.path,
		logger: o.logger.WithOptions(zap.AddCallerSkip(calls)),
	}
}

func (o *zapSink) getZapFields(fields ...Field) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	zapFields := make([]zap.Field, len(fields))
	for i, field := range fields {
		zapFields[i] = field.getZapField()
	}
	return zapFields
}

func (o *zapSink) Debug(msg string, fields ...Field) {
	o.logger.Debug(msg, o.getZapFields(fields...)...)
}

func (o *zapSink) Info(msg string, fields ...Field) {
	o.logger.Info(msg, o.getZapFields(fields...)...)
}

func (o *zapSink) Error(msg string, fields ...Field) {
	o.logger.Error(msg, o.getZapFields(fields...)...)
}

func (o *zapSink) Fatal(msg string, fields ...Field) {
	o.logger.Fatal(msg, o.getZapFields(fields...)...)
}

func (o *zapSink) Panic(msg string, fields ...Field) {
	o.logger.Panic(msg, o.getZapFields(fields...)...)
}

func (o *zapSink) Warn(msg string, fields ...Field) {
	o.logger.Warn(msg, o.getZapFields(fields...)...)
}

func (o *zapSink) Sync() error {
	return o.logger.Sync()
}

var _ Sink = &zapSink{}

type zapSyncWriter struct {
	io.Writer
}

func (w *zapSyncWriter) Sync() error {
	return nil
}
