// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"github.com/atomix/go-sdk/pkg/atomix"
	_map "github.com/atomix/go-sdk/pkg/primitive/map"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

func GetMapV1Test(name string) func(*testing.T) {
	return func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		m, err := atomix.Map[string, string](name).
			Codec(types.Scalar[string]()).
			Get(ctx)
		assert.NoError(t, err)

		streamCtx, streamCancel := context.WithCancel(context.Background())
		stream, err := m.Watch(streamCtx)
		assert.NoError(t, err)

		kv, err := m.Get(context.Background(), "foo")
		assert.Error(t, err)
		assert.True(t, errors.IsNotFound(err))
		assert.Nil(t, kv)

		size, err := m.Len(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, size)

		kv, err = m.Put(context.Background(), "foo", "bar")
		assert.NoError(t, err)
		assert.NotNil(t, kv)
		assert.Equal(t, "bar", kv.Value)

		value, err := stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		kv1, err := m.Get(context.Background(), "foo")
		assert.NoError(t, err)
		assert.NotNil(t, kv)
		assert.Equal(t, "foo", kv.Key)
		assert.Equal(t, "bar", kv.Value)

		size, err = m.Len(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, size)

		kv2, err := m.Remove(context.Background(), "foo")
		assert.NoError(t, err)
		assert.NotNil(t, kv2)
		assert.Equal(t, "foo", kv2.Key)
		assert.Equal(t, "bar", kv2.Value)
		assert.Equal(t, kv1.Version, kv2.Version)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		size, err = m.Len(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, size)

		kv, err = m.Put(context.Background(), "foo", "bar")
		assert.NoError(t, err)
		assert.NotNil(t, kv)
		assert.Equal(t, "bar", kv.Value)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		kv, err = m.Insert(context.Background(), "foo", "baz")
		assert.Error(t, err)
		assert.True(t, errors.IsAlreadyExists(err))
		assert.Nil(t, kv)

		kv, err = m.Insert(context.Background(), "bar", "baz")
		assert.NoError(t, err)
		assert.NotNil(t, kv)
		assert.Equal(t, "baz", kv.Value)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		kv, err = m.Put(context.Background(), "foo", "baz")
		assert.NoError(t, err)
		assert.NotNil(t, kv)
		assert.Equal(t, "baz", kv.Value)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		entries, err := m.List(context.Background())
		assert.NoError(t, err)
		for {
			entry, err := entries.Next()
			if err == io.EOF {
				break
			}
			assert.NotNil(t, entry)
		}

		err = m.Clear(context.Background())
		assert.NoError(t, err)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		size, err = m.Len(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, size)

		kv, err = m.Put(context.Background(), "foo", "bar")
		assert.NoError(t, err)
		assert.NotNil(t, kv)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		kv1, err = m.Get(context.Background(), "foo")
		assert.NoError(t, err)
		assert.NotNil(t, kv)

		kv2, err = m.Update(context.Background(), "foo", "baz", _map.IfVersion(kv1.Version))
		assert.NoError(t, err)
		assert.NotEqual(t, kv1.Version, kv2.Version)
		assert.Equal(t, "baz", kv2.Value)

		value, err = stream.Next()
		assert.NoError(t, err)
		assert.NotNil(t, value)

		streamCancel()

		value, err = stream.Next()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, value)

		_, err = m.Update(context.Background(), "foo", "bar", _map.IfVersion(kv1.Version))
		assert.Error(t, err)
		assert.True(t, errors.IsConflict(err))

		_, err = m.Remove(context.Background(), "foo", _map.IfVersion(kv1.Version))
		assert.Error(t, err)
		assert.True(t, errors.IsConflict(err))

		removed, err := m.Remove(context.Background(), "foo", _map.IfVersion(kv2.Version))
		assert.NoError(t, err)
		assert.NotNil(t, removed)
		assert.Equal(t, kv2.Version, removed.Version)
	}
}
