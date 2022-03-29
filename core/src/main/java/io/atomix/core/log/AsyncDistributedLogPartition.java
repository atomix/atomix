// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.log;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Asynchronous distributed log partition.
 */
public interface AsyncDistributedLogPartition<E> extends AsyncPrimitive {

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
   * @return a future to be completed once the entry has been produced
   */
  CompletableFuture<Void> produce(E entry);

  /**
   * Adds a consumer to the log partition.
   *
   * @param consumer the log partition consumer
   * @return a future to be completed once the consumer has been added
   */
  default CompletableFuture<Void> consume(Consumer<Record<E>> consumer) {
    return consume(1, consumer);
  }

  /**
   * Adds a consumer to the log partition.
   *
   * @param offset the offset from which to begin consuming the log partition
   * @param consumer the log partition consumer
   * @return a future to be completed once the consumer has been added
   */
  CompletableFuture<Void> consume(long offset, Consumer<Record<E>> consumer);

  /**
   * Returns a synchronous log partition.
   *
   * @return the synchronous log partition
   */
  default DistributedLogPartition<E> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  /**
   * Returns a synchronous log partition.
   *
   * @param operationTimeout the log partition operation timeout
   * @return the synchronous log partition
   */
  DistributedLogPartition<E> sync(Duration operationTimeout);

}
