// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	scheduler := newScheduler()

	ran := false
	scheduler.Schedule(time.UnixMilli(1), func() {
		ran = true
	})
	scheduler.tick(time.UnixMilli(1))
	assert.Equal(t, time.UnixMilli(1), scheduler.Time())
	assert.True(t, ran)

	ran = false
	scheduler.Schedule(time.UnixMilli(2), func() {
		ran = true
	})
	scheduler.tick(time.UnixMilli(3))
	assert.Equal(t, time.UnixMilli(3), scheduler.Time())
	assert.True(t, ran)

	ran = false
	scheduler.tick(time.UnixMilli(2))
	assert.Equal(t, time.UnixMilli(3), scheduler.Time())
	assert.False(t, ran)

	ran = false
	cancel := scheduler.Schedule(time.UnixMilli(5), func() {
		ran = true
	})
	scheduler.tick(time.UnixMilli(4))
	assert.Equal(t, time.UnixMilli(4), scheduler.Time())
	assert.False(t, ran)
	cancel()
	scheduler.tick(time.UnixMilli(5))
	assert.False(t, ran)
}
