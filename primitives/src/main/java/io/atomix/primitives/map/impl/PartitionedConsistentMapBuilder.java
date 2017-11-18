/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitives.map.impl;

import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitives.map.AsyncConsistentMap;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;
import java.util.function.Function;

/**
 * Partitioned consistent map builder.
 */
public class PartitionedConsistentMapBuilder<K, V> extends AbstractConsistentMapBuilder<K, V> {
  private static final int NUM_BUCKETS = 128;
  private final PartitionService partitions;

  public PartitionedConsistentMapBuilder(String name, PartitionService partitions) {
    super(name);
    this.partitions = partitions;
  }

  @Override
  public AsyncConsistentMap<K, V> buildAsync() {
    Map<PartitionId, AsyncConsistentMap<byte[], byte[]>> maps = Maps.newConcurrentMap();
    for (Partition partition : partitions.getPartitions()) {
      maps.put(partition.id(), new TranscodingAsyncConsistentMap<>(
          newRawMap(partition.getPrimitiveClient()),
          BaseEncoding.base16()::encode,
          BaseEncoding.base16()::decode,
          Function.identity(),
          Function.identity()));
    }

    Partitioner<byte[]> partitioner = key -> {
      int bucket = Math.abs(Hashing.murmur3_32().hashBytes(key).asInt()) % NUM_BUCKETS;
      return partitions.getPartitionIds().get(Hashing.consistentHash(bucket, partitions.getPartitionIds().size()));
    };

    AsyncConsistentMap<byte[], byte[]> partitionedMap = new PartitionedAsyncConsistentMap<>(name(), maps, partitioner);

    Serializer serializer = serializer();
    return new TranscodingAsyncConsistentMap<>(partitionedMap,
        key -> serializer.encode(key),
        bytes -> serializer.decode(bytes),
        value -> value == null ? null : serializer.encode(value),
        bytes -> serializer.decode(bytes));
  }
}
