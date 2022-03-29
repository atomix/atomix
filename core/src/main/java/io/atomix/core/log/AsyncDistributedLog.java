// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.primitive.AsyncPrimitive;

/**
 * Asynchronous distributed log primitive.
 */
public interface AsyncDistributedLog<E> extends AsyncPrimitive {

  /**
   * Returns the distributed log partitions.
   *
   * @return the distributed log partitions
   */
  List<AsyncDistributedLogPartition<E>> getPartitions();

  /**
   * Returns the distributed log partition for the given ID.
   *
   * @param partitionId the partition identifier
   * @return the distributed log partition
   */
  AsyncDistributedLogPartition<E> getPartition(int partitionId);

  /**
   * Returns the distributed log partition for the given entry.
   *
   * @param entry the entry for which to return the distributed log partition
   * @return the partition for the given entry
   */
  AsyncDistributedLogPartition<E> getPartition(E entry);

  /**
   * Appends an entry to the distributed log.
   *
   * @param entry the entry to append
   * @return a future to be completed once the entry has been produced
   */
  CompletableFuture<Void> produce(E entry);

  /**
   * Adds a consumer to all partitions.
   *
   * @param consumer the log consumer
   * @return a future to be completed once the consumer has been added
   */
  CompletableFuture<Void> consume(Consumer<Record<E>> consumer);

  @Override
  default DistributedLog<E> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedLog<E> sync(Duration operationTimeout);

}
