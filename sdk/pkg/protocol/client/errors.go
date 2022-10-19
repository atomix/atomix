// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"github.com/atomix/runtime/sdk/pkg/errors"
	"github.com/atomix/runtime/sdk/pkg/protocol"
)

// getErrorFromStatus creates a typed error from a response status
func getErrorFromStatus(status protocol.OperationResponseHeaders_Status, message string) error {
	switch status {
	case protocol.OperationResponseHeaders_OK:
		return nil
	case protocol.OperationResponseHeaders_ERROR:
		return errors.NewUnknown(message)
	case protocol.OperationResponseHeaders_UNKNOWN:
		return errors.NewUnknown(message)
	case protocol.OperationResponseHeaders_CANCELED:
		return errors.NewCanceled(message)
	case protocol.OperationResponseHeaders_NOT_FOUND:
		return errors.NewNotFound(message)
	case protocol.OperationResponseHeaders_ALREADY_EXISTS:
		return errors.NewAlreadyExists(message)
	case protocol.OperationResponseHeaders_UNAUTHORIZED:
		return errors.NewUnauthorized(message)
	case protocol.OperationResponseHeaders_FORBIDDEN:
		return errors.NewForbidden(message)
	case protocol.OperationResponseHeaders_CONFLICT:
		return errors.NewConflict(message)
	case protocol.OperationResponseHeaders_INVALID:
		return errors.NewInvalid(message)
	case protocol.OperationResponseHeaders_UNAVAILABLE:
		return errors.NewUnavailable(message)
	case protocol.OperationResponseHeaders_NOT_SUPPORTED:
		return errors.NewNotSupported(message)
	case protocol.OperationResponseHeaders_TIMEOUT:
		return errors.NewTimeout(message)
	case protocol.OperationResponseHeaders_FAULT:
		return errors.NewFault(message)
	case protocol.OperationResponseHeaders_INTERNAL:
		return errors.NewInternal(message)
	default:
		return errors.NewUnknown(message)
	}
}
