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
package io.atomix.primitives.tree.impl;

import com.google.common.collect.Maps;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitives.tree.AsyncDocumentTree;
import io.atomix.primitives.tree.DocumentPath;

import java.util.Map;

/**
 * Partitioned document tree builder.
 */
public class PartitionedDocumentTreeBuilder<V> extends AbstractDocumentTreeBuilder<V> {
  private static final Funnel<Iterable<? extends CharSequence>> STR_LIST_FUNNEL =
      Funnels.sequentialFunnel(Funnels.unencodedCharsFunnel());
  private static final int NUM_BUCKETS = 128;

  private final PartitionService partitions;

  public PartitionedDocumentTreeBuilder(String name, PartitionService partitions) {
    super(name);
    this.partitions = partitions;
  }

  @Override
  public AsyncDocumentTree<V> buildAsync() {
    Map<PartitionId, AsyncDocumentTree<V>> trees = Maps.newConcurrentMap();
    for (Partition partition : partitions.getPartitions()) {
      trees.put(partition.id(), newDocumentTree(partition.getPrimitiveClient()));
    }

    Partitioner<DocumentPath> partitioner = key -> {
      int bucket = (key == null) ? 0 :
          Math.abs(Hashing.murmur3_32()
              .hashObject(key.pathElements(), STR_LIST_FUNNEL)
              .asInt()) % NUM_BUCKETS;
      return partitions.getPartitionIds().get(Hashing.consistentHash(bucket, partitions.getPartitionIds().size()));
    };
    return new PartitionedAsyncDocumentTree<>(name(), trees, partitioner);
  }
}
