// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * An async atomic counter dispenses monotonically increasing values.
 */
public interface AsyncAtomicCounter extends AsyncPrimitive {

  /**
   * Atomically increment by one and return the updated value.
   *
   * @return updated value
   */
  CompletableFuture<Long> incrementAndGet();

  /**
   * Atomically decrement by one and return the updated value.
   *
   * @return updated value
   */
  CompletableFuture<Long> decrementAndGet();

  /**
   * Atomically increment by one and return the previous value.
   *
   * @return previous value
   */
  CompletableFuture<Long> getAndIncrement();

  /**
   * Atomically increment by one and return the previous value.
   *
   * @return previous value
   */
  CompletableFuture<Long> getAndDecrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return previous value
   */
  CompletableFuture<Long> getAndAdd(long delta);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return updated value
   */
  CompletableFuture<Long> addAndGet(long delta);

  /**
   * Returns the current value of the counter without modifying it.
   *
   * @return current value
   */
  CompletableFuture<Long> get();


  /**
   * Atomically sets the given value to the current value.
   *
   * @param value new value
   * @return future void
   */
  CompletableFuture<Void> set(long value);

  /**
   * Atomically sets the given counter to the updated value if the current value is the expected value, otherwise
   * no change occurs.
   *
   * @param expectedValue the expected current value of the counter
   * @param updateValue   the new value to be set
   * @return true if the update occurred and the expected value was equal to the current value, false otherwise
   */
  CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue);

  @Override
  default AtomicCounter sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicCounter sync(Duration operationTimeout);
}
