// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	benchmarks "github.com/atomix/atomix/bench/pkg/benchmark"
	"github.com/onosproject/helmit/pkg/benchmark"
)

func main() {
	benchmark.Main(map[string]benchmark.BenchmarkingSuite{
		"map": new(benchmarks.MapSuite),
	})
}
