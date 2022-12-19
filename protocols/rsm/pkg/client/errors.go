// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	protocol "github.com/atomix/atomix/protocols/rsm/pkg/api/v1"
	"github.com/atomix/atomix/runtime/pkg/errors"
)

// getErrorFromStatus creates a typed error from a response status
func getErrorFromStatus(status protocol.CallResponseHeaders_Status, message string) error {
	switch status {
	case protocol.CallResponseHeaders_OK:
		return nil
	case protocol.CallResponseHeaders_ERROR:
		return errors.NewUnknown(message)
	case protocol.CallResponseHeaders_UNKNOWN:
		return errors.NewUnknown(message)
	case protocol.CallResponseHeaders_CANCELED:
		return errors.NewCanceled(message)
	case protocol.CallResponseHeaders_NOT_FOUND:
		return errors.NewNotFound(message)
	case protocol.CallResponseHeaders_ALREADY_EXISTS:
		return errors.NewAlreadyExists(message)
	case protocol.CallResponseHeaders_UNAUTHORIZED:
		return errors.NewUnauthorized(message)
	case protocol.CallResponseHeaders_FORBIDDEN:
		return errors.NewForbidden(message)
	case protocol.CallResponseHeaders_CONFLICT:
		return errors.NewConflict(message)
	case protocol.CallResponseHeaders_INVALID:
		return errors.NewInvalid(message)
	case protocol.CallResponseHeaders_UNAVAILABLE:
		return errors.NewUnavailable(message)
	case protocol.CallResponseHeaders_NOT_SUPPORTED:
		return errors.NewNotSupported(message)
	case protocol.CallResponseHeaders_TIMEOUT:
		return errors.NewTimeout(message)
	case protocol.CallResponseHeaders_FAULT:
		return errors.NewFault(message)
	case protocol.CallResponseHeaders_INTERNAL:
		return errors.NewInternal(message)
	default:
		return errors.NewUnknown(message)
	}
}
