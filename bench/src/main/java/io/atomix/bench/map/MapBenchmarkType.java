/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench.map;

import io.atomix.bench.BenchmarkController;
import io.atomix.bench.BenchmarkExecutor;
import io.atomix.bench.BenchmarkType;
import io.atomix.core.Atomix;

/**
 * Map benchmark type.
 */
public class MapBenchmarkType implements BenchmarkType<MapBenchmarkConfig> {
  public static final MapBenchmarkType INSTANCE = new MapBenchmarkType();
  private static final String NAME = "map";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public MapBenchmarkConfig newConfig() {
    return new MapBenchmarkConfig();
  }

  @Override
  public BenchmarkController createController(Atomix atomix) {
    return new MapBenchmarkController(atomix);
  }

  @Override
  public BenchmarkExecutor createExecutor(Atomix atomix) {
    return new MapBenchmarkExecutor(atomix);
  }
}
