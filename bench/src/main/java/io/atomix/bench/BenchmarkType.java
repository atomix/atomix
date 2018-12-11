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
package io.atomix.bench;

import io.atomix.core.Atomix;

/**
 * Benchmark type.
 */
public enum BenchmarkType {
  MAP("map") {
    @Override
    public BenchmarkExecutor createExecutor(Atomix atomix) {
      return new MapBenchmarkExecutor(atomix);
    }
  };

  /**
   * Returns the benchmark type for the given type name.
   *
   * @param typeName the type name for which to return the type
   * @return the benchmark type for the given type name
   */
  public static BenchmarkType forTypeName(String typeName) {
    for (BenchmarkType type : values()) {
      if (type.typeName.equals(typeName)) {
        return type;
      }
    }
    throw new AssertionError();
  }

  private final String typeName;

  BenchmarkType(String typeName) {
    this.typeName = typeName;
  }

  /**
   * Returns the benchmark type name.
   *
   * @return the benchmark type name
   */
  String typeName() {
    return typeName;
  }

  /**
   * Creates a new executor for the benchmark type.
   *
   * @param atomix the Atomix instance
   * @return the executor for the benchmark type
   */
  public abstract BenchmarkExecutor createExecutor(Atomix atomix);
}
