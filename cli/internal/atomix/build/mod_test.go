// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestGetModFromMain(t *testing.T) {
	wd, err := os.Getwd()
	assert.NoError(t, err)
	root := filepath.Dir(filepath.Dir(filepath.Dir(wd)))

	dir, file, ok, err := getDirModFromMain(root, "cmd/atomix")
	assert.Equal(t, root, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	dir, file, ok, err = getDirModFromMain(root, "./cmd/atomix")
	assert.Equal(t, root, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	dir, file, ok, err = getDirModFromMain(root, "github.com/atomix/atomix/cli/cmd/atomix")
	assert.Equal(t, root, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	_, _, ok, err = getDirModFromMain(root, "cmd/foo")
	assert.False(t, ok)
	assert.NoError(t, err)

	_, _, ok, err = getDirModFromMain(root, "github.com/atomix/atomix/cli/cmd/foo")
	assert.False(t, ok)
	assert.NoError(t, err)

	_, _, ok, err = getDirModFromMain(root, "github.com/atomix/atomix/cli")
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestGetMod(t *testing.T) {
	wd, err := os.Getwd()
	assert.NoError(t, err)
	cliRoot := filepath.Dir(filepath.Dir(filepath.Dir(wd)))
	runtimeRoot, err := filepath.Abs(filepath.Join(cliRoot, "../runtime"))
	assert.NoError(t, err)

	dir, file, ok, err := getDirMod(cliRoot, ".")
	assert.Equal(t, cliRoot, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	dir, file, ok, err = getDirMod(cliRoot, "github.com/atomix/atomix/cli")
	assert.Equal(t, cliRoot, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	dir, file, ok, err = getDirMod(cliRoot, "../runtime")
	assert.Equal(t, runtimeRoot, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	dir, file, ok, err = getDirMod(cliRoot, runtimeRoot)
	assert.Equal(t, runtimeRoot, dir)
	assert.NotNil(t, file)
	assert.True(t, ok)
	assert.NoError(t, err)

	_, _, ok, err = getDirMod(cliRoot, "../foo")
	assert.False(t, ok)
	assert.NoError(t, err)

	_, _, ok, err = getDirMod(cliRoot, "github.com/atomix/atomix/foo")
	assert.False(t, ok)
	assert.NoError(t, err)
}
