// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package interceptors

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ErrorHandlingUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return getAtomixError(invoker(ctx, method, req, reply, cc, opts...))
	}
}

func ErrorHandlingStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &errorHandlingClientStream{
			ClientStream: stream,
		}, nil
	}
}

func ErrorHandlingUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return nil, getGRPCError(err)
		}
		return resp, nil
	}
}

func ErrorHandlingStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return getGRPCError(handler(srv, ss))
	}
}

type errorHandlingClientStream struct {
	grpc.ClientStream
}

func (s *errorHandlingClientStream) RecvMsg(m interface{}) error {
	if err := s.ClientStream.RecvMsg(m); err != nil {
		return getAtomixError(err)
	}
	return nil
}

// getAtomixError returns the given gRPC error as an Atomix error
func getAtomixError(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := err.(*errors.TypedError); ok {
		return err
	}

	status, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Unknown:
		return errors.NewUnknown(status.Message())
	case codes.Canceled:
		return errors.NewCanceled(status.Message())
	case codes.NotFound:
		return errors.NewNotFound(status.Message())
	case codes.AlreadyExists:
		return errors.NewAlreadyExists(status.Message())
	case codes.Unauthenticated:
		return errors.NewUnauthorized(status.Message())
	case codes.PermissionDenied:
		return errors.NewForbidden(status.Message())
	case codes.Aborted:
		return errors.NewConflict(status.Message())
	case codes.InvalidArgument:
		return errors.NewInvalid(status.Message())
	case codes.Unavailable:
		return errors.NewUnavailable(status.Message())
	case codes.Unimplemented:
		return errors.NewNotSupported(status.Message())
	case codes.DeadlineExceeded:
		return errors.NewTimeout(status.Message())
	case codes.Internal:
		return errors.NewInternal(status.Message())
	case codes.DataLoss:
		return errors.NewFault(status.Message())
	default:
		return err
	}
}

// getGRPCError returns the given error as a gRPC error
func getGRPCError(err error) error {
	if err == nil {
		return nil
	}

	typed, ok := err.(*errors.TypedError)
	if !ok {
		return err
	}

	switch typed.Type {
	case errors.Unknown:
		return status.Error(codes.Unknown, typed.Message)
	case errors.Canceled:
		return status.Error(codes.Canceled, typed.Message)
	case errors.NotFound:
		return status.Error(codes.NotFound, typed.Message)
	case errors.AlreadyExists:
		return status.Error(codes.AlreadyExists, typed.Message)
	case errors.Unauthorized:
		return status.Error(codes.Unauthenticated, typed.Message)
	case errors.Forbidden:
		return status.Error(codes.PermissionDenied, typed.Message)
	case errors.Conflict:
		return status.Error(codes.Aborted, typed.Message)
	case errors.Invalid:
		return status.Error(codes.InvalidArgument, typed.Message)
	case errors.Unavailable:
		return status.Error(codes.Unavailable, typed.Message)
	case errors.NotSupported:
		return status.Error(codes.Unimplemented, typed.Message)
	case errors.Timeout:
		return status.Error(codes.DeadlineExceeded, typed.Message)
	case errors.Internal:
		return status.Error(codes.Internal, typed.Message)
	case errors.Fault:
		return status.Error(codes.DataLoss, typed.Message)
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
