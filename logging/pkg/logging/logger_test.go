// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"testing"
)

func TestLoggerConfig(t *testing.T) {
	config := Config{}
	bytes, err := ioutil.ReadFile("test.yaml")
	assert.NoError(t, err)
	err = yaml.Unmarshal(bytes, &config)
	assert.NoError(t, err)
	err = configure(config)
	assert.NoError(t, err)

	// The root logger should be configured with INFO level
	logger := GetLogger()
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Infof("should be %s", "printed")

	// The "test" logger should inherit the INFO level from the root logger
	logger = GetLogger("test").WithFields(Bool("printed", true))
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debugf("should %s be", "not")
	logger.Info("should be")

	// The "test/1" logger should be configured with DEBUG level
	logger = GetLogger("test", "1")
	assert.Equal(t, DebugLevel, logger.Level())
	logger.Debugw("should be", Bool("printed", true))
	logger.Infow("should be", Bool("printed", true))

	// The "test/1/2" logger should inherit the DEBUG level from "test/1"
	logger = GetLogger("test", "1", "2").WithFields(Bool("printed", true))
	assert.Equal(t, DebugLevel, logger.Level())
	logger.Debugw("printed", String("should", "be"))
	logger.Infow("printed", String("should", "be"))

	// The "test" logger should still inherit the INFO level from the root logger
	logger = GetLogger("test")
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Info("should be printed")

	// The "test/2" logger should be configured with WARN level
	logger = GetLogger("test", "2")
	assert.Equal(t, WarnLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Infow("should not be", Bool("printed", true))
	logger.Warnw("should be printed", Int("times", 2))

	// The "test/2/3" logger should be configured with INFO level
	logger = GetLogger("test", "2", "3")
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Infow("should be printed", Int("times", 2))
	logger.Warn("should be printed twice")

	// The "test/2/4" logger should inherit the WARN level from "test/2"
	logger = GetLogger("test", "2", "4")
	assert.Equal(t, WarnLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Info("should not be printed")
	logger.Warn("should be printed twice")

	// The "test/2" logger level should be changed to DEBUG
	logger = GetLogger("test/2")
	logger.SetLevel(DebugLevel)
	assert.Equal(t, DebugLevel, logger.Level())
	logger.Debugw("should be", Bool("printed", true))
	logger.Infow("should be printed", Int("times", 2))
	logger.Warn("should be printed twice")

	// The "test/2/3" logger should not inherit the change to the "test/2" logger since its level has been explicitly set
	logger = GetLogger("test/2/3")
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Info("should be printed twice")
	logger.Warn("should be printed twice")

	// The "test/2/4" logger should inherit the change to the "test/2" logger since its level has not been explicitly set
	// The "test/2/4" logger should not output DEBUG messages since the output level is explicitly set to WARN
	logger = GetLogger("test/2/4")
	assert.Equal(t, DebugLevel, logger.Level())
	logger.Debug("should be printed")
	logger.Info("should be printed twice")
	logger.Warn("should be printed twice")

	// The "test/3" logger should be configured with INFO level
	// The "test/3" logger should write to multiple outputs
	logger = GetLogger("test/3")
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Info("should be printed")
	logger.Warn("should be printed twice")

	// The "test/3/4" logger should inherit INFO level from "test/3"
	// The "test/3/4" logger should inherit multiple outputs from "test/3"
	logger = GetLogger("test/3/4")
	assert.Equal(t, InfoLevel, logger.Level())
	logger.Debug("should not be printed")
	logger.Info("should be printed")
	logger.Warn("should be printed twice")

	//logger = GetLogger("test", "kafka")
	//assert.Equal(t, InfoLevel, logger.Level())
}
