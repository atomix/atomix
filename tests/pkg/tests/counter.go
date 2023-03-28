// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
)

type CounterTestSuite struct {
	PrimitiveTestSuite
	counterv1.CounterServer
}

func (s *CounterTestSuite) SetupSuite(ctx context.Context) {
	s.PrimitiveTestSuite.SetupSuite(ctx)
	_, err := s.Create(ctx, &counterv1.CreateRequest{
		ID: s.id,
	})
	s.NoError(err)
}

func (s *CounterTestSuite) TearDownSuite(ctx context.Context) {
	_, err := s.Close(ctx, &counterv1.CloseRequest{
		ID: s.id,
	})
	s.NoError(err)
}

func (s *CounterTestSuite) TestDefault(ctx context.Context) {
	response, err := s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(0), response.Value)
}

func (s *CounterTestSuite) TestSet(ctx context.Context) {
	setResponse, err := s.Set(ctx, &counterv1.SetRequest{
		ID:    s.id,
		Value: 1,
	})
	s.NoError(err)
	s.Equal(int64(1), setResponse.Value)

	getResponse, err := s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(1), getResponse.Value)

	setResponse, err = s.Set(ctx, &counterv1.SetRequest{
		ID:    s.id,
		Value: -1,
	})
	s.NoError(err)
	s.Equal(int64(-1), setResponse.Value)

	getResponse, err = s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(-1), getResponse.Value)
}

func (s *CounterTestSuite) TestIncrement1(ctx context.Context) {
	incResponse, err := s.Increment(ctx, &counterv1.IncrementRequest{
		ID:    s.id,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(1), incResponse.Value)

	getResponse, err := s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(1), getResponse.Value)

	incResponse, err = s.Increment(ctx, &counterv1.IncrementRequest{
		ID:    s.id,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(2), incResponse.Value)

	getResponse, err = s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(2), getResponse.Value)
}

func (s *CounterTestSuite) TestIncrementDelta(ctx context.Context) {
	incResponse, err := s.Increment(ctx, &counterv1.IncrementRequest{
		ID:    s.id,
		Delta: 10,
	})
	s.NoError(err)
	s.Equal(int64(10), incResponse.Value)

	getResponse, err := s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(10), getResponse.Value)

	incResponse, err = s.Increment(ctx, &counterv1.IncrementRequest{
		ID:    s.id,
		Delta: -20,
	})
	s.NoError(err)
	s.Equal(int64(-10), incResponse.Value)

	getResponse, err = s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(-10), getResponse.Value)
}

func (s *CounterTestSuite) TestDecrement1(ctx context.Context) {
	decResponse, err := s.Decrement(ctx, &counterv1.DecrementRequest{
		ID:    s.id,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(-1), decResponse.Value)

	getResponse, err := s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(-1), getResponse.Value)

	decResponse, err = s.Decrement(ctx, &counterv1.DecrementRequest{
		ID:    s.id,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(-2), decResponse.Value)

	getResponse, err = s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(-2), getResponse.Value)
}

func (s *CounterTestSuite) TestDecrementDelta(ctx context.Context) {
	decResponse, err := s.Decrement(ctx, &counterv1.DecrementRequest{
		ID:    s.id,
		Delta: 10,
	})
	s.NoError(err)
	s.Equal(int64(-10), decResponse.Value)

	getResponse, err := s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(-10), getResponse.Value)

	decResponse, err = s.Decrement(ctx, &counterv1.DecrementRequest{
		ID:    s.id,
		Delta: -20,
	})
	s.NoError(err)
	s.Equal(int64(10), decResponse.Value)

	getResponse, err = s.Get(ctx, &counterv1.GetRequest{
		ID: s.id,
	})
	s.NoError(err)
	s.Equal(int64(10), getResponse.Value)
}
