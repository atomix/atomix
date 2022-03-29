// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier;

import io.atomix.primitive.AsyncPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed cyclic barrier.
 */
public interface AsyncDistributedCyclicBarrier extends AsyncPrimitive {

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * @return the arrival index of the current thread, where index {@link #getParties()} - 1 indicates the first to
   * arrive and zero indicates the last to arrive
   */
  CompletableFuture<Integer> await();

  /**
   * Waits until all parties have invoked await on this barrier.
   *
   * @param timeout the time to wait for the barrier
   * @return the arrival index of the current thread, where index {@link #getParties()} - 1 indicates the first to
   * arrive and zero indicates the last to arrive
   */
  CompletableFuture<Integer> await(Duration timeout);

  /**
   * Returns the number of parties currently waiting at the barrier.
   *
   * @return the number of parties currently waiting at the barrier
   */
  CompletableFuture<Integer> getNumberWaiting();

  /**
   * Returns the number of parties required to trip this barrier.
   *
   * @return the number of parties required to trip this barrier
   */
  CompletableFuture<Integer> getParties();

  /**
   * Returns whether this barrier is in a broken state.
   *
   * @return whether this barrier is in a broken state
   */
  CompletableFuture<Boolean> isBroken();

  /**
   * Resets the barrier to its initial state.
   */
  CompletableFuture<Void> reset();

  @Override
  default DistributedCyclicBarrier sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedCyclicBarrier sync(Duration operationTimeout);
}
