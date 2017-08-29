/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitives.generator;

import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.generator.impl.DefaultAtomicIdGenerator;

import java.util.concurrent.CompletableFuture;

/**
 * An async ID generator for generating globally unique numbers.
 */
public interface AsyncAtomicIdGenerator extends DistributedPrimitive {

  @Override
  default Type primitiveType() {
    return Type.ID_GENERATOR;
  }

  /**
   * Returns the next globally unique numeric ID.
   *
   * @return a future to be completed with the next globally unique identifier
   */
  CompletableFuture<Long> nextId();

  /**
   * Returns a new {@link AtomicIdGenerator} that is backed by this instance.
   *
   * @param timeoutMillis timeout duration for the returned ConsistentMap operations
   * @return new {@code AtomicIdGenerator} instance
   */
  default AtomicIdGenerator asAtomicIdGenerator(long timeoutMillis) {
    return new DefaultAtomicIdGenerator(this, timeoutMillis);
  }

  /**
   * Returns a new {@link AtomicIdGenerator} that is backed by this instance and with a default operation timeout.
   *
   * @return new {@code AtomicIdGenerator} instance
   */
  default AtomicIdGenerator asAtomicIdGenerator() {
    return new DefaultAtomicIdGenerator(this, DEFAULT_OPERATION_TIMEOUT_MILLIS);
  }
}
