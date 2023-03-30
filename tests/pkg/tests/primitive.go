// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/atomix/atomix/runtime/pkg/utils/grpc/interceptors"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/onosproject/helmit/pkg/test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

type PrimitiveTestSuite struct {
	test.Suite
	conn *grpc.ClientConn
	ID   runtimev1.PrimitiveID
}

func (s *PrimitiveTestSuite) SetupSuite() {
	conn, err := grpc.DialContext(s.Context(), "127.0.0.1:5678",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			interceptors.ErrorHandlingUnaryClientInterceptor(),
			interceptors.RetryingUnaryClientInterceptor(interceptors.WithRetryOn(codes.Unavailable))),
		grpc.WithChainStreamInterceptor(
			interceptors.ErrorHandlingStreamClientInterceptor(),
			interceptors.RetryingStreamClientInterceptor(interceptors.WithRetryOn(codes.Unavailable))))
	s.NoError(err)
	s.conn = conn
}

func (s *PrimitiveTestSuite) TearDownSuite() {
	s.NoError(s.conn.Close())
}

func (s *PrimitiveTestSuite) SetupTest() {
	s.ID = runtimev1.PrimitiveID{
		Name: petname.Generate(2, "-"),
	}
}

func (s *PrimitiveTestSuite) Supported(err error) bool {
	if errors.IsNotSupported(err) {
		s.T().SkipNow()
		return false
	}
	return true
}

func (s *PrimitiveTestSuite) NoError(err error, args ...interface{}) bool {
	if !s.Supported(err) {
		return false
	}
	return s.Suite.NoError(err, args...)
}

func (s *PrimitiveTestSuite) NoErrorf(err error, msg string, args ...interface{}) bool {
	if !s.Supported(err) {
		return false
	}
	return s.Suite.NoErrorf(err, msg, args...)
}

func (s *PrimitiveTestSuite) Error(err error, args ...interface{}) bool {
	if !s.Supported(err) {
		return false
	}
	return s.Suite.Error(err, args...)
}

func (s *PrimitiveTestSuite) Errorf(err error, msg string, args ...interface{}) bool {
	if !s.Supported(err) {
		return false
	}
	return s.Suite.Errorf(err, msg, args...)
}

func (s *PrimitiveTestSuite) ErrorNotFound(err error, args ...interface{}) bool {
	if err == nil || !errors.IsNotFound(err) {
		return s.Error(nil, args...)
	}
	return true
}

func (s *PrimitiveTestSuite) ErrorAlreadyExists(err error, args ...interface{}) bool {
	if err == nil || !errors.IsAlreadyExists(err) {
		return s.Error(nil, args...)
	}
	return true
}

func (s *PrimitiveTestSuite) ErrorConflict(err error, args ...interface{}) bool {
	if err == nil || !errors.IsConflict(err) {
		return s.Error(nil, args...)
	}
	return true
}
