// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFactories(t *testing.T) {
	assert.Equal(t, Unknown, NewUnknown("").(*TypedError).Type)
	assert.Equal(t, "Unknown", NewUnknown("Unknown").Error())
	assert.Equal(t, Canceled, NewCanceled("").(*TypedError).Type)
	assert.Equal(t, "Canceled", NewCanceled("Canceled").Error())
	assert.Equal(t, NotFound, NewNotFound("").(*TypedError).Type)
	assert.Equal(t, "NotFound", NewNotFound("NotFound").Error())
	assert.Equal(t, AlreadyExists, NewAlreadyExists("").(*TypedError).Type)
	assert.Equal(t, "AlreadyExists", NewAlreadyExists("AlreadyExists").Error())
	assert.Equal(t, Unauthorized, NewUnauthorized("").(*TypedError).Type)
	assert.Equal(t, "Unauthorized", NewUnauthorized("Unauthorized").Error())
	assert.Equal(t, Forbidden, NewForbidden("").(*TypedError).Type)
	assert.Equal(t, "Forbidden", NewForbidden("Forbidden").Error())
	assert.Equal(t, Conflict, NewConflict("").(*TypedError).Type)
	assert.Equal(t, "Conflict", NewConflict("Conflict").Error())
	assert.Equal(t, Invalid, NewInvalid("").(*TypedError).Type)
	assert.Equal(t, "Invalid", NewInvalid("Invalid").Error())
	assert.Equal(t, Unavailable, NewUnavailable("").(*TypedError).Type)
	assert.Equal(t, "Unavailable", NewUnavailable("Unavailable").Error())
	assert.Equal(t, NotSupported, NewNotSupported("").(*TypedError).Type)
	assert.Equal(t, "NotSupported", NewNotSupported("NotSupported").Error())
	assert.Equal(t, Timeout, NewTimeout("").(*TypedError).Type)
	assert.Equal(t, "Timeout", NewTimeout("Timeout").Error())
	assert.Equal(t, Internal, NewInternal("").(*TypedError).Type)
	assert.Equal(t, "Internal", NewInternal("Internal").Error())
}

func TestPredicates(t *testing.T) {
	assert.False(t, IsUnknown(errors.New("Unknown")))
	assert.True(t, IsUnknown(NewUnknown("Unknown")))
	assert.False(t, IsCanceled(errors.New("Canceled")))
	assert.True(t, IsCanceled(NewCanceled("Canceled")))
	assert.False(t, IsNotFound(errors.New("NotFound")))
	assert.True(t, IsNotFound(NewNotFound("NotFound")))
	assert.False(t, IsAlreadyExists(errors.New("AlreadyExists")))
	assert.True(t, IsAlreadyExists(NewAlreadyExists("AlreadyExists")))
	assert.False(t, IsUnauthorized(errors.New("Unauthorized")))
	assert.True(t, IsUnauthorized(NewUnauthorized("Unauthorized")))
	assert.False(t, IsForbidden(errors.New("Forbidden")))
	assert.True(t, IsForbidden(NewForbidden("Forbidden")))
	assert.False(t, IsConflict(errors.New("Conflict")))
	assert.True(t, IsConflict(NewConflict("Conflict")))
	assert.False(t, IsInvalid(errors.New("Invalid")))
	assert.True(t, IsInvalid(NewInvalid("Invalid")))
	assert.False(t, IsUnavailable(errors.New("Unavailable")))
	assert.True(t, IsUnavailable(NewUnavailable("Unavailable")))
	assert.False(t, IsNotSupported(errors.New("NotSupported")))
	assert.True(t, IsNotSupported(NewNotSupported("NotSupported")))
	assert.False(t, IsTimeout(errors.New("Timeout")))
	assert.True(t, IsTimeout(NewTimeout("Timeout")))
	assert.False(t, IsInternal(errors.New("Internal")))
	assert.True(t, IsInternal(NewInternal("Internal")))
}
