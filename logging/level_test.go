// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLevel(t *testing.T) {
	assert.False(t, DebugLevel.Enabled(EmptyLevel))
	assert.True(t, DebugLevel.Enabled(DebugLevel))
	assert.False(t, InfoLevel.Enabled(DebugLevel))
	assert.True(t, InfoLevel.Enabled(InfoLevel))
	assert.False(t, WarnLevel.Enabled(InfoLevel))
	assert.True(t, WarnLevel.Enabled(WarnLevel))
	assert.False(t, ErrorLevel.Enabled(WarnLevel))
	assert.True(t, ErrorLevel.Enabled(ErrorLevel))
	assert.False(t, FatalLevel.Enabled(ErrorLevel))
	assert.True(t, FatalLevel.Enabled(FatalLevel))
	assert.False(t, PanicLevel.Enabled(FatalLevel))
	assert.True(t, PanicLevel.Enabled(PanicLevel))
}
