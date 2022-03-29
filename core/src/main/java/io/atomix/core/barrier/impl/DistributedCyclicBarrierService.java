// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Distributed cyclic barrier service.
 */
public interface DistributedCyclicBarrierService {

  /**
   * Joins the barrier.
   */
  @Command
  void join();

  /**
   * Invokes await on the barrier, waiting until the given timeout.
   *
   * @param timeout the timeout
   * @return the barrier instance identifier
   */
  @Command("await")
  CyclicBarrierResult<Long> await(long timeout);

  /**
   * Returns the number of parties currently waiting at the barrier.
   *
   * @return the number of parties currently waiting at the barrier
   */
  @Query
  int getNumberWaiting();

  /**
   * Returns the number of parties required to trip this barrier.
   *
   * @return the number of parties required to trip this barrier
   */
  @Query
  int getParties();

  /**
   * Returns whether the barrier is broken.
   *
   * @param barrierId the barrier instance to check
   * @return whether the barrier is broken
   */
  @Query
  boolean isBroken(long barrierId);

  /**
   * Resets the barrier.
   *
   * @param barrierId the barrier instance to reset
   */
  @Command
  void reset(long barrierId);

}
