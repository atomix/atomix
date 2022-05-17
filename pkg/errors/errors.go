// SPDX-FileCopyrightText: 2022-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Type is an error type
type Type int

const (
	// Unknown is an unknown error type
	Unknown Type = iota
	// Canceled indicates a request context was canceled
	Canceled
	// NotFound indicates a resource was not found
	NotFound
	// AlreadyExists indicates a resource already exists
	AlreadyExists
	// Unauthorized indicates access to a resource is not authorized
	Unauthorized
	// Forbidden indicates the operation requested to be performed on a resource is forbidden
	Forbidden
	// Conflict indicates a conflict occurred during concurrent modifications to a resource
	Conflict
	// Invalid indicates a message or request is invalid
	Invalid
	// Unavailable indicates a service is not available
	Unavailable
	// NotSupported indicates a method is not supported
	NotSupported
	// Timeout indicates a request timed out
	Timeout
	// Internal indicates an unexpected internal error occurred
	Internal
	// Fault indicates a data fault occurred
	Fault
)

// TypedError is an typed error
type TypedError struct {
	// Type is the error type
	Type Type
	// Message is the error message
	Message string
}

func (e *TypedError) Error() string {
	return e.Message
}

var _ error = &TypedError{}

// FromProto returns the given gRPC error as an Atomix error
func FromProto(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := err.(*TypedError); ok {
		return err
	}

	if err == context.Canceled {
		return NewCanceled(err.Error())
	}
	if err == context.DeadlineExceeded {
		return NewTimeout(err.Error())
	}

	status, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Unknown:
		return NewUnknown(status.Message())
	case codes.Canceled:
		return NewCanceled(status.Message())
	case codes.NotFound:
		return NewNotFound(status.Message())
	case codes.AlreadyExists:
		return NewAlreadyExists(status.Message())
	case codes.Unauthenticated:
		return NewUnauthorized(status.Message())
	case codes.PermissionDenied:
		return NewForbidden(status.Message())
	case codes.Aborted:
		return NewConflict(status.Message())
	case codes.InvalidArgument:
		return NewInvalid(status.Message())
	case codes.Unavailable:
		return NewUnavailable(status.Message())
	case codes.Unimplemented:
		return NewNotSupported(status.Message())
	case codes.DeadlineExceeded:
		return NewTimeout(status.Message())
	case codes.Internal:
		return NewInternal(status.Message())
	case codes.DataLoss:
		return NewFault(status.Message())
	default:
		return err
	}
}

// ToProto returns the given error as a gRPC error
func ToProto(err error) error {
	if err == nil {
		return nil
	}

	typed, ok := err.(*TypedError)
	if !ok {
		return status.Error(codes.Internal, err.Error())
	}

	switch typed.Type {
	case Unknown:
		return status.Error(codes.Unknown, typed.Message)
	case Canceled:
		return status.Error(codes.Canceled, typed.Message)
	case NotFound:
		return status.Error(codes.NotFound, typed.Message)
	case AlreadyExists:
		return status.Error(codes.AlreadyExists, typed.Message)
	case Unauthorized:
		return status.Error(codes.Unauthenticated, typed.Message)
	case Forbidden:
		return status.Error(codes.PermissionDenied, typed.Message)
	case Conflict:
		return status.Error(codes.Aborted, typed.Message)
	case Invalid:
		return status.Error(codes.InvalidArgument, typed.Message)
	case Unavailable:
		return status.Error(codes.Unavailable, typed.Message)
	case NotSupported:
		return status.Error(codes.Unimplemented, typed.Message)
	case Timeout:
		return status.Error(codes.DeadlineExceeded, typed.Message)
	case Internal:
		return status.Error(codes.Internal, typed.Message)
	case Fault:
		return status.Error(codes.DataLoss, typed.Message)
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// New creates a new typed error
func New(t Type, msg string, args ...interface{}) error {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	return &TypedError{
		Type:    t,
		Message: msg,
	}
}

// NewUnknown returns a new Unknown error
func NewUnknown(msg string, args ...interface{}) error {
	return New(Unknown, msg, args...)
}

// NewCanceled returns a new Canceled error
func NewCanceled(msg string, args ...interface{}) error {
	return New(Canceled, msg, args...)
}

// NewNotFound returns a new NotFound error
func NewNotFound(msg string, args ...interface{}) error {
	return New(NotFound, msg, args...)
}

// NewAlreadyExists returns a new AlreadyExists error
func NewAlreadyExists(msg string, args ...interface{}) error {
	return New(AlreadyExists, msg, args...)
}

// NewUnauthorized returns a new Unauthorized error
func NewUnauthorized(msg string, args ...interface{}) error {
	return New(Unauthorized, msg, args...)
}

// NewForbidden returns a new Forbidden error
func NewForbidden(msg string, args ...interface{}) error {
	return New(Forbidden, msg, args...)
}

// NewConflict returns a new Conflict error
func NewConflict(msg string, args ...interface{}) error {
	return New(Conflict, msg, args...)
}

// NewInvalid returns a new Invalid error
func NewInvalid(msg string, args ...interface{}) error {
	return New(Invalid, msg, args...)
}

// NewUnavailable returns a new Unavailable error
func NewUnavailable(msg string, args ...interface{}) error {
	return New(Unavailable, msg, args...)
}

// NewNotSupported returns a new NotSupported error
func NewNotSupported(msg string, args ...interface{}) error {
	return New(NotSupported, msg, args...)
}

// NewTimeout returns a new Timeout error
func NewTimeout(msg string, args ...interface{}) error {
	return New(Timeout, msg, args...)
}

// NewInternal returns a new Internal error
func NewInternal(msg string, args ...interface{}) error {
	return New(Internal, msg, args...)
}

// NewFault returns a new Fault error
func NewFault(msg string, args ...interface{}) error {
	return New(Fault, msg, args...)
}

// Code returns the error code
func Code(err error) int {
	return int(TypeOf(err))
}

// TypeOf returns the type of the given error
func TypeOf(err error) Type {
	if typed, ok := err.(*TypedError); ok {
		return typed.Type
	}
	return Unknown
}

// IsType checks whether the given error is of the given type
func IsType(err error, t Type) bool {
	if typed, ok := err.(*TypedError); ok {
		return typed.Type == t
	}
	return false
}

// IsUnknown checks whether the given error is an Unknown error
func IsUnknown(err error) bool {
	return IsType(err, Unknown)
}

// IsCanceled checks whether the given error is an Canceled error
func IsCanceled(err error) bool {
	return IsType(err, Canceled) || err == context.Canceled
}

// IsNotFound checks whether the given error is a NotFound error
func IsNotFound(err error) bool {
	return IsType(err, NotFound)
}

// IsAlreadyExists checks whether the given error is a AlreadyExists error
func IsAlreadyExists(err error) bool {
	return IsType(err, AlreadyExists)
}

// IsUnauthorized checks whether the given error is a Unauthorized error
func IsUnauthorized(err error) bool {
	return IsType(err, Unauthorized)
}

// IsForbidden checks whether the given error is a Forbidden error
func IsForbidden(err error) bool {
	return IsType(err, Forbidden)
}

// IsConflict checks whether the given error is a Conflict error
func IsConflict(err error) bool {
	return IsType(err, Conflict)
}

// IsInvalid checks whether the given error is an Invalid error
func IsInvalid(err error) bool {
	return IsType(err, Invalid)
}

// IsUnavailable checks whether the given error is an Unavailable error
func IsUnavailable(err error) bool {
	return IsType(err, Unavailable)
}

// IsNotSupported checks whether the given error is a NotSupported error
func IsNotSupported(err error) bool {
	return IsType(err, NotSupported)
}

// IsTimeout checks whether the given error is a Timeout error
func IsTimeout(err error) bool {
	return IsType(err, Timeout) || err == context.DeadlineExceeded
}

// IsInternal checks whether the given error is an Internal error
func IsInternal(err error) bool {
	return IsType(err, Internal)
}

// IsFault checks whether the given error is a Fault error
func IsFault(err error) bool {
	return IsType(err, Fault)
}
