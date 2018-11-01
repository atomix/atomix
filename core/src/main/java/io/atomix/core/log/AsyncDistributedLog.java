/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.log;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.primitive.AsyncPrimitive;

/**
 * Asynchronous distributed log primitive.
 */
public interface AsyncDistributedLog<E> extends AsyncPrimitive {

  /**
   * Appends an entry to the distributed log.
   *
   * @param entry the entry to append
   * @return a future to be completed once the entry has been appended
   */
  CompletableFuture<Void> produce(E entry);

  /**
   * Adds a consumer to the log.
   *
   * @param consumer the log consumer
   * @return a future to be completed once the consumer has been added
   */
  default CompletableFuture<Void> consume(Consumer<Record<E>> consumer) {
    return consume(1, consumer);
  }

  /**
   * Adds a consumer to the log.
   *
   * @param offset the offset from which to begin consuming the log
   * @param consumer the log consumer
   * @return a future to be completed once the consumer has been added
   */
  CompletableFuture<Void> consume(long offset, Consumer<Record<E>> consumer);

  @Override
  default DistributedLog<E> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedLog<E> sync(Duration operationTimeout);

}
