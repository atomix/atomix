// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
)

type CounterTestSuite struct {
	PrimitiveTestSuite
	counterv1.CounterClient
}

func (s *CounterTestSuite) SetupSuite() {
	s.PrimitiveTestSuite.SetupSuite()
	s.CounterClient = counterv1.NewCounterClient(s.conn)
}

func (s *CounterTestSuite) SetupTest() {
	s.PrimitiveTestSuite.SetupTest()
	_, err := s.Create(s.Context(), &counterv1.CreateRequest{
		ID: s.ID,
	})
	s.NoError(err)
}

func (s *CounterTestSuite) TearDownTest() {
	_, err := s.Close(s.Context(), &counterv1.CloseRequest{
		ID: s.ID,
	})
	s.NoError(err)
}

func (s *CounterTestSuite) TestDefault() {
	response, err := s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(0), response.Value)
}

func (s *CounterTestSuite) TestSet() {
	setResponse, err := s.Set(s.Context(), &counterv1.SetRequest{
		ID:    s.ID,
		Value: 1,
	})
	s.NoError(err)
	s.Equal(int64(1), setResponse.Value)

	getResponse, err := s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(1), getResponse.Value)

	setResponse, err = s.Set(s.Context(), &counterv1.SetRequest{
		ID:    s.ID,
		Value: -1,
	})
	s.NoError(err)
	s.Equal(int64(-1), setResponse.Value)

	getResponse, err = s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(-1), getResponse.Value)
}

func (s *CounterTestSuite) TestIncrement1() {
	incResponse, err := s.Increment(s.Context(), &counterv1.IncrementRequest{
		ID:    s.ID,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(1), incResponse.Value)

	getResponse, err := s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(1), getResponse.Value)

	incResponse, err = s.Increment(s.Context(), &counterv1.IncrementRequest{
		ID:    s.ID,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(2), incResponse.Value)

	getResponse, err = s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(2), getResponse.Value)
}

func (s *CounterTestSuite) TestIncrementDelta() {
	incResponse, err := s.Increment(s.Context(), &counterv1.IncrementRequest{
		ID:    s.ID,
		Delta: 10,
	})
	s.NoError(err)
	s.Equal(int64(10), incResponse.Value)

	getResponse, err := s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(10), getResponse.Value)

	incResponse, err = s.Increment(s.Context(), &counterv1.IncrementRequest{
		ID:    s.ID,
		Delta: -20,
	})
	s.NoError(err)
	s.Equal(int64(-10), incResponse.Value)

	getResponse, err = s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(-10), getResponse.Value)
}

func (s *CounterTestSuite) TestDecrement1() {
	decResponse, err := s.Decrement(s.Context(), &counterv1.DecrementRequest{
		ID:    s.ID,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(-1), decResponse.Value)

	getResponse, err := s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(-1), getResponse.Value)

	decResponse, err = s.Decrement(s.Context(), &counterv1.DecrementRequest{
		ID:    s.ID,
		Delta: 1,
	})
	s.NoError(err)
	s.Equal(int64(-2), decResponse.Value)

	getResponse, err = s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(-2), getResponse.Value)
}

func (s *CounterTestSuite) TestDecrementDelta() {
	decResponse, err := s.Decrement(s.Context(), &counterv1.DecrementRequest{
		ID:    s.ID,
		Delta: 10,
	})
	s.NoError(err)
	s.Equal(int64(-10), decResponse.Value)

	getResponse, err := s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(-10), getResponse.Value)

	decResponse, err = s.Decrement(s.Context(), &counterv1.DecrementRequest{
		ID:    s.ID,
		Delta: -20,
	})
	s.NoError(err)
	s.Equal(int64(10), decResponse.Value)

	getResponse, err = s.Get(s.Context(), &counterv1.GetRequest{
		ID: s.ID,
	})
	s.NoError(err)
	s.Equal(int64(10), getResponse.Value)
}
