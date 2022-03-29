// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import io.atomix.primitive.SyncPrimitive;

import java.util.function.Consumer;

/**
 * Distributed log partition.
 */
public interface DistributedLogPartition<E> extends SyncPrimitive {

  /**
   * Returns the partition ID.
   *
   * @return the partition ID
   */
  int id();

  /**
   * Appends an entry to the distributed log.
   *
   * @param entry the entry to append
   */
  void produce(E entry);

  /**
   * Adds a consumer to the log partition.
   *
   * @param consumer the log partition consumer
   */
  default void consume(Consumer<Record<E>> consumer) {
    consume(1, consumer);
  }

  /**
   * Adds a consumer to the log partition.
   *
   * @param offset the offset from which to begin consuming the log partition
   * @param consumer the log partition consumer
   */
  void consume(long offset, Consumer<Record<E>> consumer);

  /**
   * Returns an asynchronous API for the log partition.
   *
   * @return an asynchronous log partition
   */
  AsyncDistributedLogPartition<E> async();

}
