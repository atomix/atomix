// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

var root Logger

const pathSep = "/"

const atomixDebugEnv = "ATOMIX_DEBUG"

func init() {
	config := Config{}
	if err := load(&config); err != nil {
		panic(err)
	} else if err := configure(config); err != nil {
		panic(err)
	}
	if os.Getenv(atomixDebugEnv) != "" {
		SetLevel(DebugLevel)
	}
}

func configure(config Config) error {
	logger, err := newLogger(config)
	if err != nil {
		return err
	}
	root = logger
	return nil
}

func newLogger(config Config) (Logger, error) {
	context := newZapContext(config)
	var outputs []Output
	for name, outputConfig := range config.RootLogger.GetOutputs() {
		sink, err := context.getSink(outputConfig.GetSink())
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, newOutput(name, sink, outputConfig))
	}

	logger := &zapLogger{
		flogrLogger: &flogrLogger{
			zapContext: context,
			config:     config.RootLogger,
		},
		outputs: outputs,
	}
	if config.RootLogger.Level != nil {
		logger.level.Store(int32(levelStringToLevel(*config.RootLogger.Level)))
	}
	return logger, nil
}

// GetLogger gets a logger by name
func GetLogger(path ...string) Logger {
	if len(path) > 1 {
		panic("number of paths must be 0 or 1")
	}
	if len(path) == 0 {
		pkg, ok := getCallerPackage()
		if !ok {
			panic("could not retrieve logger package")
		}
		path = []string{pkg}
	}
	return root.GetLogger(path[0])
}

// getCallerPackage gets the package name of the calling function'ss caller
func getCallerPackage() (string, bool) {
	var pkg string
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return pkg, false
	}
	parts := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	if parts[len(parts)-2][0] == '(' {
		pkg = strings.Join(parts[0:len(parts)-2], ".")
	} else {
		pkg = strings.Join(parts[0:len(parts)-1], ".")
	}
	return pkg, true
}

// SetLevel sets the root logger level
func SetLevel(level Level) {
	root.SetLevel(level)
}

// Logger represents an abstract logging interface.
type Logger interface {
	// Name returns the logger name
	Name() string

	// GetLogger gets a descendant of this Logger
	GetLogger(path string) Logger

	// Level returns the logger's level
	Level() Level

	// SetLevel sets the logger's level
	SetLevel(level Level)

	// WithFields adds fields to the logger
	WithFields(fields ...Field) Logger

	// WithOutputs adds outputs to the logger
	WithOutputs(outputs ...Output) Logger

	// WithSkipCalls skipsthe given number of calls to the logger methods
	WithSkipCalls(calls int) Logger

	// Sync flushes the logger
	Sync() error

	Debug(...interface{})
	Debugf(template string, args ...interface{})
	Debugw(msg string, fields ...Field)

	Info(...interface{})
	Infof(template string, args ...interface{})
	Infow(msg string, fields ...Field)

	Warn(...interface{})
	Warnf(template string, args ...interface{})
	Warnw(msg string, fields ...Field)

	Error(...interface{})
	Errorf(template string, args ...interface{})
	Errorw(msg string, fields ...Field)

	Fatal(...interface{})
	Fatalf(template string, args ...interface{})
	Fatalw(msg string, fields ...Field)

	Panic(...interface{})
	Panicf(template string, args ...interface{})
	Panicw(msg string, fields ...Field)
}

func newZapContext(config Config) *zapContext {
	return &zapContext{
		config: config,
	}
}

type zapContext struct {
	config Config
	sinks  sync.Map
	mu     sync.Mutex
}

func (c *zapContext) getSink(name string) (Sink, error) {
	sink, ok := c.sinks.Load(name)
	if ok {
		return sink.(Sink), nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	sink, ok = c.sinks.Load(name)
	if ok {
		return sink.(Sink), nil
	}

	config, ok := c.config.GetSink(name)
	if !ok {
		return nil, fmt.Errorf("sink %s not found", name)
	}

	sink, err := newSink(config)
	if err != nil {
		return nil, err
	}
	c.sinks.Store(name, sink)
	return sink.(Sink), nil
}

type flogrLogger struct {
	*zapContext
	config       LoggerConfig
	path         string
	children     sync.Map
	mu           sync.Mutex
	level        atomic.Int32
	defaultLevel atomic.Int32
}

func (l *flogrLogger) Level() Level {
	level := Level(l.level.Load())
	if level != EmptyLevel {
		return level
	}
	return Level(l.defaultLevel.Load())
}

func (l *flogrLogger) SetLevel(level Level) {
	l.level.Store(int32(level))
	l.children.Range(func(key, value any) bool {
		value.(*zapLogger).setDefaultLevel(level)
		return true
	})
}

func (l *flogrLogger) setDefaultLevel(level Level) {
	l.defaultLevel.Store(int32(level))
	if Level(l.level.Load()) == EmptyLevel {
		l.children.Range(func(key, value any) bool {
			value.(*zapLogger).setDefaultLevel(level)
			return true
		})
	}
}

// zapLogger is the default Logger implementation
type zapLogger struct {
	*flogrLogger
	outputs []Output
}

func (l *zapLogger) Name() string {
	return l.path
}

func (l *zapLogger) GetLogger(path string) Logger {
	logger := l
	names := strings.Split(path, pathSep)
	for _, name := range names {
		child, err := logger.getChild(name)
		if err != nil {
			panic(err)
		}
		logger = child
	}
	return logger
}

func (l *zapLogger) getChild(name string) (*zapLogger, error) {
	child, ok := l.children.Load(name)
	if ok {
		return child.(*zapLogger), nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	child, ok = l.children.Load(name)
	if ok {
		return child.(*zapLogger), nil
	}

	path := strings.Trim(fmt.Sprintf("%s/%s", l.path, name), pathSep)

	var outputs []Output
	outputNames := make(map[string]int)
	for i, output := range l.outputs {
		outputs = append(outputs, output.getChild(name))
		if output.Name() != "" {
			outputNames[output.Name()] = i
		}
	}

	// Initialize the child logger's configuration if one is not set.
	loggerConfig, ok := l.zapContext.config.GetLogger(path)
	if ok {
		for outputName, outputConfig := range loggerConfig.GetOutputs() {
			var output Output
			if i, ok := outputNames[outputName]; ok {
				if outputConfig.Sink == nil {
					output = outputs[i].getChild(name)
					if outputConfig.Level != nil {
						output = output.WithLevel(outputConfig.GetLevel())
					}
				} else {
					sink, err := l.getSink(outputConfig.GetSink())
					if err != nil {
						return nil, err
					}
					output = newOutput(outputName, sink, outputConfig).getChild(name)
					if outputConfig.Level == nil {
						output = output.WithLevel(outputs[i].Level())
					}
				}
				outputs[i] = output
			} else {
				sink, err := l.getSink(outputConfig.GetSink())
				if err != nil {
					return nil, err
				}
				output = newOutput(outputName, sink, outputConfig).getChild(name)
				outputs = append(outputs, output)
			}
		}
	}

	defaultLevel := Level(l.defaultLevel.Load())
	level := Level(l.level.Load())
	if level != EmptyLevel {
		defaultLevel = level
	}

	// Create the child logger.
	logger := &zapLogger{
		flogrLogger: &flogrLogger{
			zapContext: l.zapContext,
			config:     loggerConfig,
			path:       path,
		},
		outputs: outputs,
	}

	// Set the logger level.
	logger.defaultLevel.Store(int32(defaultLevel))
	if loggerConfig.Level != nil {
		logger.SetLevel(loggerConfig.GetLevel())
	}

	// Cache the child logger.
	l.children.Store(name, logger)
	return logger, nil
}

func (l *zapLogger) WithOutputs(outputs ...Output) Logger {
	allOutputs := make([]Output, 0, len(l.outputs)+len(outputs))
	for _, output := range l.outputs {
		allOutputs = append(allOutputs, output)
	}
	for _, output := range outputs {
		allOutputs = append(allOutputs, output.getChild(l.path))
	}
	return &zapLogger{
		flogrLogger: l.flogrLogger,
		outputs:     allOutputs,
	}
}

func (l *zapLogger) WithFields(fields ...Field) Logger {
	outputs := make([]Output, len(l.outputs))
	for i, output := range l.outputs {
		outputs[i] = output.WithFields(fields...)
	}
	return &zapLogger{
		flogrLogger: l.flogrLogger,
		outputs:     outputs,
	}
}

func (l *zapLogger) WithSkipCalls(calls int) Logger {
	outputs := make([]Output, len(l.outputs))
	for i, output := range l.outputs {
		outputs[i] = output.WithSkipCalls(calls)
	}
	return &zapLogger{
		flogrLogger: l.flogrLogger,
		outputs:     outputs,
	}
}

func (l *zapLogger) Sync() error {
	var err error
	for _, output := range l.outputs {
		err = output.Sync()
	}
	return err
}

func (l *zapLogger) log(level Level, template string, args []interface{}, fields []Field, logger func(output Output, msg string, fields []Field)) {
	if l.Level() > level {
		return
	}

	msg := template
	if msg == "" && len(args) > 0 {
		msg = fmt.Sprint(args...)
	} else if msg != "" && len(args) > 0 {
		msg = fmt.Sprintf(template, args...)
	}

	for _, output := range l.outputs {
		logger(output, msg, fields)
	}
}

func (l *zapLogger) Debug(args ...interface{}) {
	l.log(DebugLevel, "", args, nil, func(output Output, msg string, fields []Field) {
		output.Debug(msg, fields...)
	})
}

func (l *zapLogger) Debugf(template string, args ...interface{}) {
	l.log(DebugLevel, template, args, nil, func(output Output, msg string, fields []Field) {
		output.Debug(msg, fields...)
	})
}

func (l *zapLogger) Debugw(msg string, fields ...Field) {
	l.log(DebugLevel, "", nil, fields, func(output Output, _ string, fields []Field) {
		output.Debug(msg, fields...)
	})
}

func (l *zapLogger) Info(args ...interface{}) {
	l.log(InfoLevel, "", args, nil, func(output Output, msg string, fields []Field) {
		output.Info(msg, fields...)
	})
}

func (l *zapLogger) Infof(template string, args ...interface{}) {
	l.log(InfoLevel, template, args, nil, func(output Output, msg string, fields []Field) {
		output.Info(msg, fields...)
	})
}

func (l *zapLogger) Infow(msg string, fields ...Field) {
	l.log(InfoLevel, "", nil, fields, func(output Output, _ string, fields []Field) {
		output.Info(msg, fields...)
	})
}

func (l *zapLogger) Warn(args ...interface{}) {
	l.log(WarnLevel, "", args, nil, func(output Output, msg string, fields []Field) {
		output.Warn(msg, fields...)
	})
}

func (l *zapLogger) Warnf(template string, args ...interface{}) {
	l.log(WarnLevel, template, args, nil, func(output Output, msg string, fields []Field) {
		output.Warn(msg, fields...)
	})
}

func (l *zapLogger) Warnw(msg string, fields ...Field) {
	l.log(WarnLevel, "", nil, fields, func(output Output, _ string, fields []Field) {
		output.Warn(msg, fields...)
	})
}

func (l *zapLogger) Error(args ...interface{}) {
	l.log(ErrorLevel, "", args, nil, func(output Output, msg string, fields []Field) {
		output.Error(msg, fields...)
	})
}

func (l *zapLogger) Errorf(template string, args ...interface{}) {
	l.log(ErrorLevel, template, args, nil, func(output Output, msg string, fields []Field) {
		output.Error(msg, fields...)
	})
}

func (l *zapLogger) Errorw(msg string, fields ...Field) {
	l.log(ErrorLevel, "", nil, fields, func(output Output, _ string, fields []Field) {
		output.Error(msg, fields...)
	})
}

func (l *zapLogger) Fatal(args ...interface{}) {
	l.log(FatalLevel, "", args, nil, func(output Output, msg string, fields []Field) {
		output.Fatal(msg, fields...)
	})
}

func (l *zapLogger) Fatalf(template string, args ...interface{}) {
	l.log(FatalLevel, template, args, nil, func(output Output, msg string, fields []Field) {
		output.Fatal(msg, fields...)
	})
}

func (l *zapLogger) Fatalw(msg string, fields ...Field) {
	l.log(FatalLevel, "", nil, fields, func(output Output, _ string, fields []Field) {
		output.Fatal(msg, fields...)
	})
}

func (l *zapLogger) Panic(args ...interface{}) {
	l.log(PanicLevel, "", args, nil, func(output Output, msg string, fields []Field) {
		output.Panic(msg, fields...)
	})
}

func (l *zapLogger) Panicf(template string, args ...interface{}) {
	l.log(PanicLevel, template, args, nil, func(output Output, msg string, fields []Field) {
		output.Panic(msg, fields...)
	})
}

func (l *zapLogger) Panicw(msg string, fields ...Field) {
	l.log(PanicLevel, "", nil, fields, func(output Output, _ string, fields []Field) {
		output.Panic(msg, fields...)
	})
}

var _ Logger = &zapLogger{}
