// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import io.atomix.primitive.SyncPrimitive;

import java.util.List;
import java.util.function.Consumer;

/**
 * Distributed log primitive.
 */
public interface DistributedLog<E> extends SyncPrimitive {

  /**
   * Returns the distributed log partitions.
   *
   * @return the distributed log partitions
   */
  List<DistributedLogPartition<E>> getPartitions();

  /**
   * Returns the distributed log partition for the given ID.
   *
   * @param partitionId the partition identifier
   * @return the distributed log partition
   */
  DistributedLogPartition<E> getPartition(int partitionId);

  /**
   * Returns the distributed log partition for the given entry.
   *
   * @param entry the entry for which to return the distributed log partition
   * @return the partition for the given entry
   */
  DistributedLogPartition<E> getPartition(E entry);

  /**
   * Appends an entry to the distributed log.
   *
   * @param entry the entry to append
   */
  void produce(E entry);

  /**
   * Adds a consumer to all partitions.
   *
   * @param consumer the log consumer
   */
  void consume(Consumer<Record<E>> consumer);

  @Override
  AsyncDistributedLog<E> async();

}
