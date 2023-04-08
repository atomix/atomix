// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package logging

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSinkConsoleEncoding(t *testing.T) {
	buf := &bytes.Buffer{}
	sink, err := NewSink(buf, WithEncoding(ConsoleEncoding), WithMessageKey("message"))
	assert.NoError(t, err)
	assert.NotNil(t, sink)

	sink.Info("foo")
	assert.NoError(t, sink.Sync())
	assert.Equal(t, "foo\n", buf.String())
}

func TestSinkJSONEncoding(t *testing.T) {
	buf := &bytes.Buffer{}
	sink, err := NewSink(buf, WithEncoding(JSONEncoding), WithMessageKey("message"))
	assert.NoError(t, err)
	assert.NotNil(t, sink)

	sink.Info("foo")
	assert.NoError(t, sink.Sync())
	assert.Equal(t, "{\"message\":\"foo\"}\n", buf.String())
}

func TestSinkFields(t *testing.T) {
	buf := &bytes.Buffer{}
	sink, err := NewSink(buf, WithEncoding(JSONEncoding))
	assert.NoError(t, err)
	assert.NotNil(t, sink)

	sink = sink.WithFields(String("foo", "bar"), Int("baz", 1))
	sink.Info("Hello world!")
	assert.NoError(t, sink.Sync())
	assert.Equal(t, "{\"foo\":\"bar\",\"baz\":1}\n", buf.String())
}
