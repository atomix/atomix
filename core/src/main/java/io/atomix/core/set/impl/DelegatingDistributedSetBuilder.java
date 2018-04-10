/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.set.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.impl.CachingAsyncConsistentMap;
import io.atomix.core.map.impl.ConsistentMapProxy;
import io.atomix.core.map.impl.PartitionedAsyncConsistentMap;
import io.atomix.core.map.impl.TranscodingAsyncConsistentMap;
import io.atomix.core.map.impl.UnmodifiableAsyncConsistentMap;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DelegatingDistributedSetBuilder<E> extends DistributedSetBuilder<E> {
  private static final int NUM_BUCKETS = 128;

  public DelegatingDistributedSetBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    return managementService.getPrimitiveRegistry().createPrimitive(name(), primitiveType())
        .thenCompose(info -> {
          PartitionGroup<?> partitions = managementService.getPartitionService().getPartitionGroup(protocol);

          Map<PartitionId, CompletableFuture<AsyncConsistentMap<byte[], byte[]>>> maps = Maps.newConcurrentMap();
          for (Partition partition : partitions.getPartitions()) {
            maps.put(partition.id(), partition.getPrimitiveClient()
                .newProxy(name(), primitiveType(), protocol)
                .connect()
                .thenApply(proxy -> new TranscodingAsyncConsistentMap<>(
                    new ConsistentMapProxy(proxy),
                    BaseEncoding.base16()::encode,
                    BaseEncoding.base16()::decode,
                    Function.identity(),
                    Function.identity())));
          }

          Partitioner<byte[]> partitioner = key -> {
            int bucket = Math.abs(Hashing.murmur3_32().hashBytes(key).asInt()) % NUM_BUCKETS;
            return partitions.getPartitionIds().get(Hashing.consistentHash(bucket, partitions.getPartitionIds().size()));
          };

          return Futures.allOf(Lists.newArrayList(maps.values()))
              .thenApply(m -> {
                AsyncConsistentMap<byte[], byte[]> partitionedMap = new PartitionedAsyncConsistentMap<>(name(), Maps.transformValues(maps, v -> v.getNow(null)), partitioner);

                Serializer serializer = serializer();
                AsyncConsistentMap<E, Boolean> map = new TranscodingAsyncConsistentMap<>(partitionedMap,
                    key -> serializer.encode(key),
                    bytes -> serializer.decode(bytes),
                    value -> value == null ? null : serializer.encode(value),
                    bytes -> serializer.decode(bytes));

                if (config.isCacheEnabled()) {
                  map = new CachingAsyncConsistentMap<>(map, config.getCacheSize());
                }

                if (config.isReadOnly()) {
                  map = new UnmodifiableAsyncConsistentMap<>(map);
                }
                return map.sync();
              });
        })
        .thenApply(map -> new DelegatingAsyncDistributedSet<>(map.async()).sync());
  }
}
