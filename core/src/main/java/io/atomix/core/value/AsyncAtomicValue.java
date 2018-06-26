/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.core.value;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicReference.
 * <p>
 * All methods of this interface return a {@link CompletableFuture future} immediately
 * after a successful invocation. The operation itself is executed asynchronous and
 * the returned future will be {@link CompletableFuture#complete completed} when the
 * operation finishes.
 *
 * @param <V> value type
 */
public interface AsyncAtomicValue<V> extends AsyncPrimitive {

  /**
   * Atomically sets the value to the given updated value if the current value is equal to the expected value.
   * <p>
   * IMPORTANT: Equality is based on the equality of the serialized {code byte[]} representations.
   * <p>
   *
   * @param expect the expected value
   * @param update the new value
   * @return CompletableFuture that will be completed with {@code true} if update was successful. Otherwise future
   * will be completed with a value of {@code false}
   */
  CompletableFuture<Boolean> compareAndSet(V expect, V update);

  /**
   * Gets the current value.
   *
   * @return CompletableFuture that will be completed with the value
   */
  CompletableFuture<V> get();

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param value the new value
   * @return CompletableFuture that will be completed with the previous value
   */
  CompletableFuture<V> getAndSet(V value);

  /**
   * Sets to the given value.
   *
   * @param value value to set
   * @return CompletableFuture that will be completed when the operation finishes
   */
  CompletableFuture<Void> set(V value);

  /**
   * Registers the specified listener to be notified whenever the atomic value is updated.
   *
   * @param listener listener to notify about events
   * @return CompletableFuture that will be completed when the operation finishes
   */
  CompletableFuture<Void> addListener(AtomicValueEventListener<V> listener);

  /**
   * Unregisters the specified listener such that it will no longer
   * receive atomic value update notifications.
   *
   * @param listener listener to unregister
   * @return CompletableFuture that will be completed when the operation finishes
   */
  CompletableFuture<Void> removeListener(AtomicValueEventListener<V> listener);

  @Override
  default AtomicValue<V> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicValue<V> sync(Duration operationTimeout);
}
