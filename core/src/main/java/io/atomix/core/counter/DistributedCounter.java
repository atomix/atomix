// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter;

import io.atomix.primitive.SyncPrimitive;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicLong.
 */
public interface DistributedCounter extends SyncPrimitive {

  /**
   * Atomically increment by one and return the updated value.
   *
   * @return updated value
   */
  long incrementAndGet();

  /**
   * Atomically decrement by one and return the updated value.
   *
   * @return updated value
   */
  long decrementAndGet();

  /**
   * Atomically increment by one and return the previous value.
   *
   * @return previous value
   */
  long getAndIncrement();

  /**
   * Atomically decrement by one and return the previous value.
   *
   * @return previous value
   */
  long getAndDecrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return previous value
   */
  long getAndAdd(long delta);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return updated value
   */
  long addAndGet(long delta);

  /**
   * Returns the current value of the counter without modifying it.
   *
   * @return current value
   */
  long get();

  @Override
  AsyncDistributedCounter async();
}
