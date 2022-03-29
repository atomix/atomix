// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier;

import io.atomix.primitive.SyncPrimitive;

import java.time.Duration;

/**
 * Distributed cyclic barrier.
 */
public interface DistributedCyclicBarrier extends SyncPrimitive {

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * @return the arrival index of the current thread, where index {@link #getParties()} - 1 indicates the first to
   * arrive and zero indicates the last to arrive
   */
  int await();

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * @param timeout the time to wait for the barrier
   * @return the arrival index of the current thread, where index {@link #getParties()} - 1 indicates the first to
   * arrive and zero indicates the last to arrive
   */
  int await(Duration timeout);

  /**
   * Returns the number of parties currently waiting at the barrier.
   *
   * @return the number of parties currently waiting at the barrier
   */
  int getNumberWaiting();

  /**
   * Returns the number of parties required to trip this barrier.
   *
   * @return the number of parties required to trip this barrier
   */
  int getParties();

  /**
   * Returns whether this barrier is in a broken state.
   *
   * @return whether this barrier is in a broken state
   */
  boolean isBroken();

  /**
   * Resets the barrier to its initial state.
   */
  void reset();

  @Override
  AsyncDistributedCyclicBarrier async();
}
