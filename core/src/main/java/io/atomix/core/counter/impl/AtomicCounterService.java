// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Atomic counter service.
 */
public interface AtomicCounterService {

  /**
   * Atomically increment by one and return the updated value.
   *
   * @return updated value
   */
  @Command
  long incrementAndGet();

  /**
   * Atomically decrement by one and return the updated value.
   *
   * @return updated value
   */
  @Command
  long decrementAndGet();

  /**
   * Atomically increment by one and return the previous value.
   *
   * @return previous value
   */
  @Command
  long getAndIncrement();

  /**
   * Atomically decrement by one and return the previous value.
   *
   * @return previous value
   */
  @Command
  long getAndDecrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return previous value
   */
  @Command
  long getAndAdd(long delta);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return updated value
   */
  @Command
  long addAndGet(long delta);

  /**
   * Atomically sets the given value to the current value.
   *
   * @param value the value to set
   */
  @Command
  void set(long value);

  /**
   * Atomically sets the given counter to the updated value if the current value is the expected value, otherwise
   * no change occurs.
   *
   * @param expectedValue the expected current value of the counter
   * @param updateValue   the new value to be set
   * @return true if the update occurred and the expected value was equal to the current value, false otherwise
   */
  @Command
  boolean compareAndSet(long expectedValue, long updateValue);

  /**
   * Returns the current value of the counter without modifying it.
   *
   * @return current value
   */
  @Query
  long get();

}
