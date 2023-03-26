// SPDX-FileCopyrightText: 2023-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"context"
	"github.com/atomix/go-sdk/pkg/atomix"
	_map "github.com/atomix/go-sdk/pkg/primitive/map"
	"github.com/atomix/go-sdk/pkg/types"
	petname "github.com/dustinkirkland/golang-petname"
	"github.com/onosproject/helmit/pkg/benchmark"
	"math/rand"
)

const numKeys = 1000

type MapSuite struct {
	benchmark.Suite
	_map _map.Map[string, string]
	keys []string
}

func (s *MapSuite) BenchmarkPut(ctx context.Context) error {
	_, err := s._map.Put(ctx, s.keys[rand.Intn(len(s.keys))], "Hello world!")
	return err
}

func (s *MapSuite) BenchmarkGet(ctx context.Context) error {
	_, err := s._map.Get(ctx, s.keys[rand.Intn(len(s.keys))])
	return err
}

func (s *MapSuite) SetupWorker(ctx context.Context) error {
	m, err := atomix.Map[string, string]("benchmark-map").
		Codec(types.Scalar[string]()).
		Get(ctx)
	if err != nil {
		return err
	}
	s._map = m

	for i := 0; i < numKeys; i++ {
		s.keys = append(s.keys, petname.Generate(2, "-"))
	}
	return nil
}

func (s *MapSuite) TearDownWorker(ctx context.Context) error {
	return s._map.Close(ctx)
}

var _ benchmark.SetupWorker = (*MapSuite)(nil)
var _ benchmark.TearDownWorker = (*MapSuite)(nil)
