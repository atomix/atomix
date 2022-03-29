// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import java.util.List;

/**
 * Interface for mapping from an object to partition ID.
 *
 * @param <K> object type.
 */
@FunctionalInterface
public interface Partitioner<K> {

  /**
   * Murmur 3 partitioner.
   */
  Partitioner<String> MURMUR3 = new Murmur3Partitioner();

  /**
   * Returns the partition ID to which the specified object maps.
   *
   * @param key        the key to partition
   * @param partitions the list of partitions
   * @return partition identifier
   */
  PartitionId partition(K key, List<PartitionId> partitions);

}
