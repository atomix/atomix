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
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"testing"
	"time"
)

func Run(t *testing.T, tests PrimitiveSuite, timeout time.Duration) {
	defer recoverAndFailOnPanic(t)

	testPrimitive(t, tests, timeout)

	var suiteSetupDone bool

	methodFinder := reflect.TypeOf(tests)
	suiteName := methodFinder.Elem().Name()

	for i := 0; i < methodFinder.NumMethod(); i++ {
		method := methodFinder.Method(i)

		ok, err := methodFilter(method.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "testify: invalid regexp for -m: %s\n", err)
			os.Exit(1)
		}

		if !ok {
			continue
		}

		if !suiteSetupDone {
			if setupAllSuite, ok := tests.(suite.SetupAllSuite); ok {
				setupAllSuite.SetupSuite()
			}
			suiteSetupDone = true
		}

		t.Run(method.Name, func(t *testing.T) {
			parentT := tests.T()
			tests.SetT(t)

			defer recoverAndFailOnPanic(t)
			defer func() {
				r := recover()

				if afterTestSuite, ok := tests.(suite.AfterTest); ok {
					afterTestSuite.AfterTest(suiteName, method.Name)
				}

				if tearDownTestSuite, ok := tests.(suite.TearDownTestSuite); ok {
					tearDownTestSuite.TearDownTest()
				}

				tests.SetT(parentT)
				failOnPanic(t, r)
			}()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			tests.SetID(runtimev1.PrimitiveID{
				Name: fmt.Sprintf("%s-%s", suiteName, method.Name),
			})
			tests.SetContext(ctx)
			assert.NoError(t, tests.CreatePrimitive())

			if setupTestSuite, ok := tests.(suite.SetupTestSuite); ok {
				setupTestSuite.SetupTest()
			}
			if beforeTestSuite, ok := tests.(suite.BeforeTest); ok {
				beforeTestSuite.BeforeTest(methodFinder.Elem().Name(), method.Name)
			}

			method.Func.Call([]reflect.Value{reflect.ValueOf(tests)})
		})
	}
	if suiteSetupDone {
		defer func() {
			if tearDownAllSuite, ok := tests.(suite.TearDownAllSuite); ok {
				tearDownAllSuite.TearDownSuite()
			}
		}()
	}
}

func testPrimitive(t *testing.T, tests PrimitiveSuite, timeout time.Duration) {
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
}

func methodFilter(name string) (bool, error) {
	return regexp.MatchString("^Test", name)
}

func recoverAndFailOnPanic(t *testing.T) {
	r := recover()
	failOnPanic(t, r)
}

func failOnPanic(t *testing.T, r interface{}) {
	if r != nil {
		t.Errorf("test panicked: %v\n%s", r, debug.Stack())
		t.FailNow()
	}
}
