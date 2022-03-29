// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.counter;

import com.google.common.annotations.Beta;

/**
 * Gossip-based counter service.
 */
@Beta
public interface CounterDelegate {

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
   * Returns the counter value.
   *
   * @return the counter value
   */
  long get();

  /**
   * Closes the counter.
   */
  void close();
}
