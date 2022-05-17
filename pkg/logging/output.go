// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"bytes"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/url"
	"strings"
	"sync"
)

func newZapOutput(logger LoggerConfig, output OutputConfig, sink SinkConfig) (*zapOutput, error) {
	zapConfig := zap.Config{}
	zapConfig.Level = levelToAtomicLevel(output.GetLevel())
	zapConfig.Encoding = string(sink.GetEncoding())
	zapConfig.EncoderConfig.EncodeName = zapcore.FullNameEncoder
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.EncoderConfig.EncodeDuration = zapcore.NanosDurationEncoder
	zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	zapConfig.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	zapConfig.EncoderConfig.NameKey = "logger"
	zapConfig.EncoderConfig.MessageKey = "message"
	zapConfig.EncoderConfig.LevelKey = "level"
	zapConfig.EncoderConfig.TimeKey = "timestamp"
	zapConfig.EncoderConfig.CallerKey = "caller"
	zapConfig.EncoderConfig.StacktraceKey = "trace"

	var encoder zapcore.Encoder
	switch sink.GetEncoding() {
	case ConsoleEncoding:
		encoder = zapcore.NewConsoleEncoder(zapConfig.EncoderConfig)
	case JSONEncoding:
		encoder = zapcore.NewJSONEncoder(zapConfig.EncoderConfig)
	}

	var path string
	switch sink.GetType() {
	case StdoutSinkType:
		path = StdoutSinkType.String()
	case StderrSinkType:
		path = StderrSinkType.String()
	case FileSinkType:
		path = sink.GetFileSinkConfig().Path
	case KafkaSinkType:
		kafkaConfig := sink.GetKafkaSinkConfig()
		var rawQuery bytes.Buffer
		if kafkaConfig.Topic != "" {
			rawQuery.WriteString("topic=")
			rawQuery.WriteString(kafkaConfig.Topic)
		}

		if kafkaConfig.Key != "" {
			rawQuery.WriteString("&")
			rawQuery.WriteString("key=")
			rawQuery.WriteString(kafkaConfig.Key)
		}
		kafkaURL := url.URL{Scheme: KafkaSinkType.String(), Host: strings.Join(kafkaConfig.Brokers, ","), RawQuery: rawQuery.String()}
		path = kafkaURL.String()
	}

	writer, err := getWriter(path)
	if err != nil {
		return nil, err
	}

	atomLevel := zap.AtomicLevel{}
	switch output.GetLevel() {
	case DebugLevel:
		atomLevel = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case InfoLevel:
		atomLevel = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case WarnLevel:
		atomLevel = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case ErrorLevel:
		atomLevel = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	case PanicLevel:
		atomLevel = zap.NewAtomicLevelAt(zapcore.PanicLevel)
	case FatalLevel:
		atomLevel = zap.NewAtomicLevelAt(zapcore.FatalLevel)
	}

	zapLogger, err := zapConfig.Build(zap.AddCallerSkip(4))
	if err != nil {
		return nil, err
	}

	zapLogger = zapLogger.WithOptions(
		zap.WrapCore(
			func(zapcore.Core) zapcore.Core {
				return zapcore.NewCore(encoder, writer, &atomLevel)
			}))
	return &zapOutput{
		config: output,
		logger: zapLogger.Named(logger.Name),
	}, nil
}

var writers = make(map[string]zapcore.WriteSyncer)
var writersMu = &sync.Mutex{}

func getWriter(url string) (zapcore.WriteSyncer, error) {
	writersMu.Lock()
	defer writersMu.Unlock()
	writer, ok := writers[url]
	if !ok {
		ws, _, err := zap.Open(url)
		if err != nil {
			return nil, err
		}
		writer = ws
		writers[url] = writer
	}
	return writer, nil
}

// Output is a logging output
type Output interface {
	WithFields(fields ...Field) Output
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	Panic(msg string, fields ...Field)
	DPanic(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Sync() error
}

// zapOutput is a logging output implementation
type zapOutput struct {
	config OutputConfig
	logger *zap.Logger
}

func (o *zapOutput) WithFields(fields ...Field) Output {
	return &zapOutput{
		config: o.config,
		logger: o.logger.With(o.getZapFields(fields...)...),
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
	o.logger.Debug(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) Info(msg string, fields ...Field) {
	o.logger.Info(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) Error(msg string, fields ...Field) {
	o.logger.Error(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) Fatal(msg string, fields ...Field) {
	o.logger.Fatal(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) Panic(msg string, fields ...Field) {
	o.logger.Panic(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) DPanic(msg string, fields ...Field) {
	o.logger.DPanic(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) Warn(msg string, fields ...Field) {
	o.logger.Warn(msg, o.getZapFields(fields...)...)
}

func (o *zapOutput) Sync() error {
	return o.logger.Sync()
}

var _ Output = &zapOutput{}
