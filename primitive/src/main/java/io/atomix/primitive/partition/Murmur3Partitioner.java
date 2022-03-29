// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import com.google.common.hash.Hashing;

import java.util.List;

/**
 * Murmur 3 partitioner.
 */
public class Murmur3Partitioner implements Partitioner<String> {
  @Override
  public PartitionId partition(String key, List<PartitionId> partitions) {
    int hash = Math.abs(Hashing.murmur3_32().hashUnencodedChars(key).asInt());
    return partitions.get(Hashing.consistentHash(hash, partitions.size()));
  }
}
