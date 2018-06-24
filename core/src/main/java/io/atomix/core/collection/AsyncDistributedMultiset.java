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
package io.atomix.core.collection;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveType;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed multiset.
 */
public interface AsyncDistributedMultiset<E> extends AsyncDistributedCollection<E> {
  @Override
  default PrimitiveType type() {
    return DistributedMultisetType.instance();
  }

  /**
   * Registers the specified listener to be notified whenever
   * the set is updated.
   *
   * @param listener listener to notify about set update events
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> addListener(SetEventListener<E> listener);

  /**
   * Unregisters the specified listener.
   *
   * @param listener listener to unregister.
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> removeListener(SetEventListener<E> listener);

  @Override
  default DistributedMultiset<E> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedMultiset<E> sync(Duration operationTimeout);
}
