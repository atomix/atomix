// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/cenkalti/backoff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"reflect"
	"testing"
	"time"
)

func NewTestFunc(tests PrimitiveSuite, timeout time.Duration) func(*testing.T) {
	return func(t *testing.T) {
		tests.SetT(t)
		tests.SetID(runtimev1.PrimitiveID{
			Name: t.Name(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		tests.SetContext(ctx)
		err := backoff.Retry(func() error {
			err := tests.CreatePrimitive()
			if errors.IsUnavailable(err) {
				return err
			}
			return backoff.Permanent(err)
		}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
		if errors.IsNotSupported(err) {
			t.Skip(reflect.TypeOf(tests).Elem().Name())
		}
		_ = tests.ClosePrimitive()
		if t.Failed() {
			t.Fail()
		}
		cancel()

		suite.Run(t, &TestRunner{
			PrimitiveSuite: tests,
		})
	}
}

type PrimitiveSuite interface {
	PrimitiveTesting
	CreatePrimitive() error
	ClosePrimitive() error
}

type PrimitiveTesting interface {
	suite.TestingSuite
	SetID(id runtimev1.PrimitiveID)
	ID() runtimev1.PrimitiveID
	SetContext(ctx context.Context)
	Context() context.Context
}

type PrimitiveTests struct {
	suite.Suite
	id  runtimev1.PrimitiveID
	ctx context.Context
}

func (t *PrimitiveTests) SetID(id runtimev1.PrimitiveID) {
	t.id = id
}

func (t *PrimitiveTests) ID() runtimev1.PrimitiveID {
	return t.id
}

func (t *PrimitiveTests) SetContext(ctx context.Context) {
	t.ctx = ctx
}

func (t *PrimitiveTests) Context() context.Context {
	return t.ctx
}

func (t *PrimitiveTests) SkipNow() {
	t.T().SkipNow()
}

func (t *PrimitiveTests) Supported(err error) bool {
	if errors.IsNotSupported(err) {
		t.SkipNow()
		return false
	}
	return true
}

func (t *PrimitiveTests) NoError(err error, args ...interface{}) bool {
	if !t.Supported(err) {
		return false
	}
	return t.Suite.NoError(err, args...)
}

func (t *PrimitiveTests) NoErrorf(err error, msg string, args ...interface{}) bool {
	if !t.Supported(err) {
		return false
	}
	return t.Suite.NoErrorf(err, msg, args...)
}

func (t *PrimitiveTests) Error(err error, args ...interface{}) bool {
	if !t.Supported(err) {
		return false
	}
	return t.Suite.Error(err, args...)
}

func (t *PrimitiveTests) Errorf(err error, msg string, args ...interface{}) bool {
	if !t.Supported(err) {
		return false
	}
	return t.Suite.Errorf(err, msg, args...)
}

func (t *PrimitiveTests) ErrorNotFound(err error, args ...interface{}) bool {
	if err == nil || !errors.IsNotFound(err) {
		return t.Error(nil, args...)
	}
	return true
}

func (t *PrimitiveTests) ErrorAlreadyExists(err error, args ...interface{}) bool {
	if err == nil || !errors.IsAlreadyExists(err) {
		return t.Error(nil, args...)
	}
	return true
}

func (t *PrimitiveTests) ErrorConflict(err error, args ...interface{}) bool {
	if err == nil || !errors.IsConflict(err) {
		return t.Error(nil, args...)
	}
	return true
}

type TestRunner struct {
	PrimitiveSuite
	ctx     context.Context
	cancel  context.CancelFunc
	timeout time.Duration
}

func (s *TestRunner) BeforeTest(_, testName string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	s.ctx = ctx
	s.cancel = cancel
	s.SetID(runtimev1.PrimitiveID{
		Name: fmt.Sprintf("%s-%s", reflect.TypeOf(s.PrimitiveSuite).Elem().Name(), testName),
	})
	assert.NoError(s.T(), s.CreatePrimitive())
}

func (s *TestRunner) AfterTest(_, _ string) {
	assert.NoError(s.T(), s.ClosePrimitive())
	s.cancel()
}
