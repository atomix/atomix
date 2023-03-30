// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package benchmarks

import (
	"bytes"
	"context"
	mapv1 "github.com/atomix/atomix/api/runtime/map/v1"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/onosproject/helmit/pkg/benchmark"
	"math/rand"
)

const (
	defaultNumKeys   = 1000
	defaultValueSize = 128
	alphabet         = "abcdefghijklmnopqrstuvwxyz"
)

type MapBenchmarkSuite struct {
	PrimitiveBenchmarkSuite
	mapv1.MapClient
	keys  []string
	value []byte
}

func (s *MapBenchmarkSuite) BenchmarkPut(ctx context.Context) error {
	_, err := s.Put(ctx, &mapv1.PutRequest{
		ID:    s.ID,
		Key:   s.keys[rand.Intn(len(s.keys))],
		Value: s.value,
	})
	return err
}

func (s *MapBenchmarkSuite) BenchmarkGet(ctx context.Context) error {
	_, err := s.Get(ctx, &mapv1.GetRequest{
		ID:  s.ID,
		Key: s.keys[rand.Intn(len(s.keys))],
	})
	return err
}

func (s *MapBenchmarkSuite) SetupBenchmark(ctx context.Context) error {
	if err := s.PrimitiveBenchmarkSuite.SetupTest(ctx); err != nil {
		return err
	}
	_, err := s.Create(ctx, &mapv1.CreateRequest{
		ID: s.ID,
	})
	return err
}

func (s *MapBenchmarkSuite) TearDownBenchmark(ctx context.Context) error {
	_, err := s.Close(ctx, &mapv1.CloseRequest{
		ID: s.ID,
	})
	return err
}

func (s *MapBenchmarkSuite) SetupWorker(ctx context.Context) error {
	if err := s.PrimitiveBenchmarkSuite.SetupWorker(ctx); err != nil {
		return err
	}
	numKeys := s.Arg("keys").Int()
	if numKeys == 0 {
		numKeys = defaultNumKeys
	}
	for i := 0; i < numKeys; i++ {
		s.keys = append(s.keys, petname.Generate(2, "-"))
	}

	valueSize := s.Arg("value-size").Int()
	if valueSize == 0 {
		valueSize = defaultValueSize
	}
	buf := &bytes.Buffer{}
	for i := 0; i < valueSize; i++ {
		buf.WriteByte(alphabet[rand.Intn(len(alphabet))])
	}
	s.value = buf.Bytes()
	return nil
}

var _ benchmark.SetupBenchmark = (*MapBenchmarkSuite)(nil)
var _ benchmark.TearDownBenchmark = (*MapBenchmarkSuite)(nil)
var _ benchmark.SetupWorker = (*MapBenchmarkSuite)(nil)
var _ benchmark.TearDownWorker = (*MapBenchmarkSuite)(nil)
