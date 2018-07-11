/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.core.counter;

import io.atomix.primitive.AsyncPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicLong.
 */
public interface AsyncDistributedCounter extends AsyncPrimitive {

  /**
   * Increments the distributed counter.
   *
   * @return the counter value
   */
  CompletableFuture<Long> incrementAndGet();

  /**
   * Returns the current value of the counter without modifying it.
   *
   * @return current value
   */
  CompletableFuture<Long> get();

  @Override
  default DistributedCounter sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedCounter sync(Duration operationTimeout);
}
