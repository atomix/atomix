/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
