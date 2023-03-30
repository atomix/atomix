// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

// SinkType is a type of sink
type SinkType string

func (t SinkType) String() string {
	return string(t)
}

const (
	// StdoutSinkType is the sink type for stdout
	StdoutSinkType SinkType = "stdout"
	// StderrSinkType is the sink type for stderr
	StderrSinkType SinkType = "stderr"
	// FileSinkType is the type for a file sink
	FileSinkType SinkType = "file"
)

// SinkEncoding is the encoding for a sink
type SinkEncoding string

func (e SinkEncoding) String() string {
	return string(e)
}

const (
	// ConsoleEncoding is an encoding for outputs to the console
	ConsoleEncoding SinkEncoding = "console"
	// JSONEncoding is an encoding for JSON outputs
	JSONEncoding SinkEncoding = "json"
)

const (
	rootLoggerName = "root"
)

// Config logging configuration
type Config struct {
	Loggers map[string]LoggerConfig `json:"loggers" yaml:"loggers"`
	Sinks   map[string]SinkConfig   `json:"sinks" yaml:"sinks"`
}

// GetRootLogger returns the root logger configuration
func (c Config) GetRootLogger() LoggerConfig {
	root := c.Loggers[rootLoggerName]
	if root.Output == nil {
		defaultSink := ""
		root.Output = map[string]OutputConfig{
			"": {
				Sink: &defaultSink,
			},
		}
	}
	return root
}

// GetLoggers returns the configured loggers
func (c Config) GetLoggers() []LoggerConfig {
	loggers := make([]LoggerConfig, 0, len(c.Loggers))
	for name, logger := range c.Loggers {
		if name != rootLoggerName {
			logger.Name = name
			if logger.Output == nil {
				logger.Output = map[string]OutputConfig{}
			}
			loggers = append(loggers, logger)
		}
	}
	return loggers
}

// GetLogger returns a logger by name
func (c Config) GetLogger(name string) (LoggerConfig, bool) {
	if name == rootLoggerName {
		return LoggerConfig{}, false
	}

	logger, ok := c.Loggers[name]
	if ok {
		logger.Name = name
		if logger.Output == nil {
			logger.Output = map[string]OutputConfig{}
		}
		return logger, true
	}
	return LoggerConfig{Output: map[string]OutputConfig{}}, false
}

// GetSinks returns the configured sinks
func (c Config) GetSinks() []SinkConfig {
	sinks := []SinkConfig{
		{
			Name: "",
		},
	}
	for name, sink := range c.Sinks {
		sink.Name = name
		sinks = append(sinks, sink)
	}
	return sinks
}

// GetSink returns a sink by name
func (c Config) GetSink(name string) (SinkConfig, bool) {
	if name == "" {
		return SinkConfig{
			Name: "",
		}, true
	}
	sink, ok := c.Sinks[name]
	if ok {
		sink.Name = name
		return sink, true
	}
	return SinkConfig{}, false
}

// LoggerConfig is the configuration for a logger
type LoggerConfig struct {
	Name   string                  `json:"name" yaml:"name"`
	Level  *string                 `json:"level,omitempty" yaml:"level,omitempty"`
	Output map[string]OutputConfig `json:"output" yaml:"output"`
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
func (c LoggerConfig) GetOutputs() []OutputConfig {
	outputs := c.Output
	outputsList := make([]OutputConfig, 0, len(outputs))
	for name, output := range outputs {
		output.Name = name
		outputsList = append(outputsList, output)
	}
	return outputsList
}

// GetOutput returns an output by name
func (c LoggerConfig) GetOutput(name string) (OutputConfig, bool) {
	output, ok := c.Output[name]
	if ok {
		output.Name = name
		return output, true
	}
	return OutputConfig{}, false
}

// OutputConfig is the configuration for a sink output
type OutputConfig struct {
	Name  string  `json:"name" yaml:"name"`
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
	Name     string            `json:"name" yaml:"name"`
	Encoding *SinkEncoding     `json:"encoding,omitempty" yaml:"encoding,omitempty"`
	Stdout   *StdoutSinkConfig `json:"stdout" yaml:"stdout,omitempty"`
	Stderr   *StderrSinkConfig `json:"stderr" yaml:"stderr,omitempty"`
	File     *FileSinkConfig   `json:"file" yaml:"file,omitempty"`
}

// GetType returns the sink type
func (c SinkConfig) GetType() SinkType {
	if c.Stdout != nil {
		return StdoutSinkType
	} else if c.Stderr != nil {
		return StderrSinkType
	} else if c.File != nil {
		return FileSinkType
	}
	return StdoutSinkType
}

// GetEncoding returns the sink encoding
func (c SinkConfig) GetEncoding() SinkEncoding {
	encoding := c.Encoding
	if encoding != nil {
		return *encoding
	}
	return ConsoleEncoding
}

// GetStdoutSinkConfig returns the stdout sink configuration
func (c SinkConfig) GetStdoutSinkConfig() StdoutSinkConfig {
	config := c.Stdout
	if config != nil {
		return *config
	}
	return StdoutSinkConfig{}
}

// GetStderrSinkConfig returns the stderr sink configuration
func (c SinkConfig) GetStderrSinkConfig() StderrSinkConfig {
	config := c.Stderr
	if config != nil {
		return *config
	}
	return StderrSinkConfig{}
}

// GetFileSinkConfig returns the file sink configuration
func (c SinkConfig) GetFileSinkConfig() FileSinkConfig {
	config := c.File
	if config != nil {
		return *config
	}
	return FileSinkConfig{}
}

// StdoutSinkConfig is the configuration for an stdout sink
type StdoutSinkConfig struct {
}

// StderrSinkConfig is the configuration for an stderr sink
type StderrSinkConfig struct {
}

// FileSinkConfig is the configuration for a file sink
type FileSinkConfig struct {
	Path string `json:"path" yaml:"path"`
}

// load loads the configuration
func load(config *Config) error {
	bytes, err := os.ReadFile("logging.yaml")
	if err == nil {
		return yaml.Unmarshal(bytes, config)
	} else if !os.IsNotExist(err) {
		return err
	}

	bytes, err = os.ReadFile(".atomix/logging.yaml")
	if err == nil {
		return yaml.Unmarshal(bytes, config)
	} else if !os.IsNotExist(err) {
		return err
	}

	home, err := homedir.Dir()
	if err == nil {
		bytes, err = os.ReadFile(filepath.Join(home, ".atomix/logging.yaml"))
		if err == nil {
			return yaml.Unmarshal(bytes, config)
		} else if !os.IsNotExist(err) {
			return err
		}
	}

	bytes, err = os.ReadFile("/etc/atomix/logging.yaml")
	if err == nil {
		return yaml.Unmarshal(bytes, config)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}
