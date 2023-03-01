// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	counterv1 "github.com/atomix/atomix/api/runtime/counter/v1"
	runtimecounterv1 "github.com/atomix/atomix/runtime/pkg/runtime/counter/v1"
	runtime "github.com/atomix/atomix/runtime/pkg/runtime/v1"
)

func NewCounterTests(runtime *runtime.Runtime) PrimitiveSuite {
	return &CounterTests{
		PrimitiveTests: &PrimitiveTests{},
		CounterServer:  runtimecounterv1.NewCounterServer(runtime),
	}
}

type CounterTests struct {
	*PrimitiveTests
	counterv1.CounterServer
}

func (t *CounterTests) CreatePrimitive() error {
	_, err := t.Create(t.Context(), &counterv1.CreateRequest{
		ID: t.ID(),
	})
	return err
}

func (t *CounterTests) ClosePrimitive() error {
	_, err := t.Close(t.Context(), &counterv1.CloseRequest{
		ID: t.ID(),
	})
	return err
}

func (t *CounterTests) TestDefault() {
	response, err := t.Get(t.ctx, &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(0), response.Value)
}

func (t *CounterTests) TestSet() {
	setResponse, err := t.Set(t.Context(), &counterv1.SetRequest{
		ID:    t.ID(),
		Value: 1,
	})
	t.NoError(err)
	t.Equal(int64(1), setResponse.Value)

	getResponse, err := t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(1), getResponse.Value)

	setResponse, err = t.Set(t.Context(), &counterv1.SetRequest{
		ID:    t.ID(),
		Value: -1,
	})
	t.NoError(err)
	t.Equal(int64(1), setResponse.Value)

	getResponse, err = t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-1), getResponse.Value)
}

func (t *CounterTests) TestIncrement() {
	incResponse, err := t.Increment(t.Context(), &counterv1.IncrementRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(1), incResponse.Value)

	getResponse, err := t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(1), getResponse.Value)

	incResponse, err = t.Increment(t.Context(), &counterv1.IncrementRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(2), incResponse.Value)

	getResponse, err = t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(2), getResponse.Value)
}

func (t *CounterTests) TestIncrementDelta() {
	incResponse, err := t.Increment(t.Context(), &counterv1.IncrementRequest{
		ID:    t.ID(),
		Delta: 10,
	})
	t.NoError(err)
	t.Equal(int64(10), incResponse.Value)

	getResponse, err := t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(10), getResponse.Value)

	incResponse, err = t.Increment(t.Context(), &counterv1.IncrementRequest{
		ID:    t.ID(),
		Delta: -20,
	})
	t.NoError(err)
	t.Equal(int64(-10), incResponse.Value)

	getResponse, err = t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-20), getResponse.Value)
}

func (t *CounterTests) TestDecrement() {
	decResponse, err := t.Decrement(t.Context(), &counterv1.DecrementRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-1), decResponse.Value)

	getResponse, err := t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-1), getResponse.Value)

	decResponse, err = t.Decrement(t.Context(), &counterv1.DecrementRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-2), decResponse.Value)

	getResponse, err = t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-2), getResponse.Value)
}

func (t *CounterTests) TestDecrementDelta() {
	decResponse, err := t.Decrement(t.Context(), &counterv1.DecrementRequest{
		ID:    t.ID(),
		Delta: 10,
	})
	t.NoError(err)
	t.Equal(int64(-10), decResponse.Value)

	getResponse, err := t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(-10), getResponse.Value)

	decResponse, err = t.Decrement(t.Context(), &counterv1.DecrementRequest{
		ID:    t.ID(),
		Delta: -20,
	})
	t.NoError(err)
	t.Equal(int64(10), decResponse.Value)

	getResponse, err = t.Get(t.Context(), &counterv1.GetRequest{
		ID: t.ID(),
	})
	t.NoError(err)
	t.Equal(int64(20), getResponse.Value)
}
