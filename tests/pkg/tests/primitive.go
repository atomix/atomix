// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/onosproject/helmit/pkg/test"
)

type PrimitiveTestSuite struct {
	test.Suite
	id runtimev1.PrimitiveID
}

func (s *PrimitiveTestSuite) SetupSuite(ctx context.Context) {
	s.id = runtimev1.PrimitiveID{
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
