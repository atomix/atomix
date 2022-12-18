// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRunAsync(t *testing.T) {
	values := []string{
		"one",
		"two",
		"three",
	}
	err := IterAsync(len(values), func(i int) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestExecuteAsync(t *testing.T) {
	values := []string{
		"one",
		"two",
		"three",
	}
	results, err := ExecuteAsync(len(values), func(i int) (interface{}, error) {
		return values[i], nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.NotEqual(t, "", results[0].(string))
	assert.NotEqual(t, "", results[1].(string))
	assert.NotEqual(t, "", results[2].(string))
}

func TestExecuteOrderedAsync(t *testing.T) {
	values := []string{
		"one",
		"two",
		"three",
	}
	results, err := ExecuteOrderedAsync(len(values), func(i int) (interface{}, error) {
		return values[i], nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, "one", results[0].(string))
	assert.Equal(t, "two", results[1].(string))
	assert.Equal(t, "three", results[2].(string))
}
