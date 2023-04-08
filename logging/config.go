// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"github.com/mitchellh/go-homedir"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

// SinkType is a type of sink
type SinkType string

func (t SinkType) String() string {
	return string(t)
}

// Encoding is the encoding for a sink
type Encoding string

func (e Encoding) String() string {
	return string(e)
}

const (
	// ConsoleEncoding is an encoding for outputs to the console
	ConsoleEncoding Encoding = "console"
	// JSONEncoding is an encoding for JSON outputs
	JSONEncoding Encoding = "json"
)

// Config logging configuration
type Config struct {
	zapcore.EncoderConfig `json:",inline" yaml:",inline"`
	RootLogger            LoggerConfig            `json:"rootLogger" yaml:"rootLogger"`
	Loggers               map[string]LoggerConfig `json:"loggers" yaml:"loggers"`
	Sinks                 map[string]SinkConfig   `json:"sinks" yaml:"sinks"`
}

// GetLogger returns a logger by name
func (c Config) GetLogger(name string) (LoggerConfig, bool) {
	if c.Loggers == nil {
		return LoggerConfig{}, false
	}
	logger, ok := c.Loggers[name]
	if ok {
		if logger.Outputs == nil {
			logger.Outputs = map[string]OutputConfig{}
		}
		return logger, true
	}
	return LoggerConfig{Outputs: map[string]OutputConfig{}}, false
}

const (
	defaultNameKey    = "logger"
	defaultMessageKey = "message"
	defaultLevelKey   = "level"
	defaultTimeKey    = "timestamp"
)

// GetSink returns a sink by name
func (c Config) GetSink(name string) (SinkConfig, bool) {
	if c.Sinks == nil {
		return SinkConfig{
			EncoderConfig: c.EncoderConfig,
		}, false
	}
	sink, ok := c.Sinks[name]
	if !ok {
		return SinkConfig{
			EncoderConfig: c.EncoderConfig,
		}, false
	}

	if c.MessageKey == "" {
		c.MessageKey = defaultMessageKey
	}
	if c.LevelKey == "" {
		c.LevelKey = defaultLevelKey
	}
	if c.TimeKey == "" {
		c.TimeKey = defaultTimeKey
	}
	if c.NameKey == "" {
		c.NameKey = defaultNameKey
	}

	if sink.MessageKey == "" {
		sink.MessageKey = c.MessageKey
	}
	if sink.LevelKey == "" {
		sink.LevelKey = c.LevelKey
	}
	if sink.TimeKey == "" {
		sink.TimeKey = c.TimeKey
	}
	if sink.NameKey == "" {
		sink.NameKey = c.NameKey
	}
	if sink.CallerKey == "" {
		sink.CallerKey = c.CallerKey
	}
	if sink.FunctionKey == "" {
		sink.FunctionKey = c.FunctionKey
	}
	if sink.StacktraceKey == "" {
		sink.StacktraceKey = c.StacktraceKey
	}
	if sink.LineEnding == "" {
		sink.LineEnding = c.LineEnding
	}
	if sink.EncodeLevel == nil {
		sink.EncodeLevel = c.EncodeLevel
	}
	if sink.EncodeTime == nil {
		sink.EncodeTime = c.EncodeTime
	}
	if sink.EncodeDuration == nil {
		sink.EncodeDuration = c.EncodeDuration
	}
	if sink.EncodeCaller == nil {
		sink.EncodeCaller = c.EncodeCaller
	}
	if sink.EncodeName == nil {
		sink.EncodeName = c.EncodeName
	}
	return sink, ok
}

// LoggerConfig is the configuration for a logger
type LoggerConfig struct {
	Level   *string                 `json:"level,omitempty" yaml:"level,omitempty"`
	Outputs map[string]OutputConfig `json:"outputs" yaml:"outputs"`
}

// GetLevel returns the logger level
func (c LoggerConfig) GetLevel() Level {
	level := c.Level
	if level != nil {
		return levelStringToLevel(*level)
	}
	return ErrorLevel
}

// GetOutputs returns the logger outputs
func (c LoggerConfig) GetOutputs() map[string]OutputConfig {
	outputs := c.Outputs
	if outputs == nil {
		outputs = make(map[string]OutputConfig)
	}
	return outputs
}

// GetOutput returns an output by name
func (c LoggerConfig) GetOutput(name string) (OutputConfig, bool) {
	output, ok := c.Outputs[name]
	return output, ok
}

// OutputConfig is the configuration for a sink output
type OutputConfig struct {
	Sink  *string `json:"sink,omitempty" yaml:"sink,omitempty"`
	Level *string `json:"level,omitempty" yaml:"level,omitempty"`
}

// GetSink returns the output sink
func (c OutputConfig) GetSink() string {
	sink := c.Sink
	if sink != nil {
		return *sink
	}
	return ""
}

// GetLevel returns the output level
func (c OutputConfig) GetLevel() Level {
	level := c.Level
	if level != nil {
		return levelStringToLevel(*level)
	}
	return DebugLevel
}

// SinkConfig is the configuration for a sink
type SinkConfig struct {
	zapcore.EncoderConfig `json:",inline" yaml:",inline"`
	Path                  string    `json:"path,omitempty" yaml:"path,omitempty"`
	Encoding              *Encoding `json:"encoding,omitempty" yaml:"encoding,omitempty"`
}

// GetEncoding returns the sink encoding
func (c SinkConfig) GetEncoding() Encoding {
	encoding := c.Encoding
	if encoding != nil {
		return *encoding
	}
	return ConsoleEncoding
}

// load the flogr configuration
func load(config *Config) error {
	bytes, err := os.ReadFile("flogr.yaml")
	if err == nil {
		return yaml.Unmarshal(bytes, config)
	} else if !os.IsNotExist(err) {
		return err
	}

	bytes, err = os.ReadFile(filepath.Join(".atomix", "flogr.yaml"))
	if err == nil {
		return yaml.Unmarshal(bytes, config)
	} else if !os.IsNotExist(err) {
		return err
	}

	home, err := homedir.Dir()
	if err == nil {
		bytes, err = os.ReadFile(filepath.Join(home, ".atomix", "flogr.yaml"))
		if err == nil {
			return yaml.Unmarshal(bytes, config)
		} else if !os.IsNotExist(err) {
			return err
		}
	}

	bytes, err = os.ReadFile("/etc/atomix/flogr.yaml")
	if err == nil {
		return yaml.Unmarshal(bytes, config)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}
