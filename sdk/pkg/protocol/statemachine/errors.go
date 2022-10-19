// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol"
)

// getFailure gets the proto status for the given error
func getFailure(err error) *protocol.Failure {
	if err == nil {
		return nil
	}
	return &protocol.Failure{
		Status:  getStatus(err),
		Message: getMessage(err),
	}
}

func getStatus(err error) protocol.Failure_Status {
	typed, ok := err.(*errors.TypedError)
	if !ok {
		return protocol.Failure_ERROR
	}

	switch typed.Type {
	case errors.Unknown:
		return protocol.Failure_UNKNOWN
	case errors.Canceled:
		return protocol.Failure_CANCELED
	case errors.NotFound:
		return protocol.Failure_NOT_FOUND
	case errors.AlreadyExists:
		return protocol.Failure_ALREADY_EXISTS
	case errors.Unauthorized:
		return protocol.Failure_UNAUTHORIZED
	case errors.Forbidden:
		return protocol.Failure_FORBIDDEN
	case errors.Conflict:
		return protocol.Failure_CONFLICT
	case errors.Invalid:
		return protocol.Failure_INVALID
	case errors.Unavailable:
		return protocol.Failure_UNAVAILABLE
	case errors.NotSupported:
		return protocol.Failure_NOT_SUPPORTED
	case errors.Timeout:
		return protocol.Failure_TIMEOUT
	case errors.Fault:
		return protocol.Failure_FAULT
	case errors.Internal:
		return protocol.Failure_INTERNAL
	default:
		return protocol.Failure_ERROR
	}
}

// getMessage gets the message for the given error
func getMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
