// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOutputLevels(t *testing.T) {
	buf := &bytes.Buffer{}
	sink, err := NewSink(buf, WithEncoding(ConsoleEncoding), WithMessageKey("message"))
	assert.NoError(t, err)

	output := NewOutput(sink, WithLevel(InfoLevel))
	output.Debug("foo")
	assert.NoError(t, output.Sync())
	assert.Equal(t, "", buf.String())

	output.Info("foo")
	assert.NoError(t, output.Sync())
	assert.Equal(t, "foo\n", buf.String())

	output = output.WithLevel(DebugLevel)
	output.Debug("bar")
	assert.NoError(t, output.Sync())
	assert.Equal(t, "foo\nbar\n", buf.String())
}
