// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.barrier.impl;

import io.atomix.primitive.event.Event;

/**
 * Distributed cyclic barrier client.
 */
public interface DistributedCyclicBarrierClient {

  /**
   * Notifies the client that the barrier is broken.
   *
   * @param id the barrier instance identifier
   */
  @Event
  void broken(long id);

  /**
   * Releases the client from waiting.
   *
   * @param id the barrier instance identifier
   * @param index the index at which the client entered the barrier
   */
  @Event
  void release(long id, int index);

  /**
   * Notifies this client to run the barrier action.
   */
  @Event
  void runAction();

}
