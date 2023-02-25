// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"github.com/atomix/go-sdk/pkg/atomix"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func GetCounterV1Test(name string, timeout time.Duration) func(*testing.T) {
	return func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		counter, err := atomix.Counter(name).Get(ctx)
		assert.NoError(t, err)

		value, err := counter.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), value)

		err = counter.Set(ctx, 1)
		assert.NoError(t, err)

		value, err = counter.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), value)

		err = counter.Set(ctx, -1)
		assert.NoError(t, err)

		value, err = counter.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), value)

		value, err = counter.Increment(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), value)

		value, err = counter.Decrement(ctx, 10)
		assert.NoError(t, err)
		assert.Equal(t, int64(-10), value)

		value, err = counter.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(-10), value)

		value, err = counter.Increment(ctx, 20)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), value)

		err = counter.Close(ctx)
		assert.NoError(t, err)

		counter, err = atomix.Counter(name).Get(ctx)
		assert.NoError(t, err)

		value, err = counter.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), value)

		err = counter.Close(ctx)
		assert.NoError(t, err)
	}
}
