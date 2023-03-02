// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	"github.com/stretchr/testify/suite"
)

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
