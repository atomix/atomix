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
package io.atomix.tree.impl;

import com.google.common.collect.Maps;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.tree.AsyncDocumentTree;
import io.atomix.tree.DocumentPath;
import io.atomix.tree.DocumentTreeBuilder;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default {@link AsyncDocumentTree} builder.
 *
 * @param <V> type for document tree value
 */
public class DocumentTreeProxyBuilder<V> extends DocumentTreeBuilder<V> {
  private static final Funnel<Iterable<? extends CharSequence>> STR_LIST_FUNNEL =
      Funnels.sequentialFunnel(Funnels.unencodedCharsFunnel());
  private static final int NUM_BUCKETS = 128;

  private final PrimitiveManagementService managementService;

  public DocumentTreeProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  protected AsyncDocumentTree<V> newDocumentTree(PrimitiveProxy proxy) {
    DocumentTreeProxy rawTree = new DocumentTreeProxy(proxy.open().join());
    AsyncDocumentTree<V> documentTree = new TranscodingAsyncDocumentTree<>(
        rawTree,
        serializer()::encode,
        serializer()::decode);
    if (relaxedReadConsistency()) {
      documentTree = new CachingAsyncDocumentTree<V>(documentTree);
    }
    return documentTree;
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncDocumentTree<V> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    PartitionGroup partitions = managementService.getPartitionService().getPartitionGroup(protocol);

    Map<PartitionId, AsyncDocumentTree<V>> trees = Maps.newConcurrentMap();
    for (Partition partition : partitions.getPartitions()) {
      trees.put(partition.id(), newDocumentTree(partition.getPrimitiveClient().proxyBuilder(name(), primitiveType(), protocol).build()));
    }

    Partitioner<DocumentPath> partitioner = key -> {
      int bucket = (key == null) ? 0 :
          Math.abs(Hashing.murmur3_32()
              .hashUnencodedChars(key.pathElements().size() == 1 ? key.pathElements().get(0) : key.pathElements().get(1))
              .asInt()) % NUM_BUCKETS;
      return partitions.getPartitionIds().get(Hashing.consistentHash(bucket, partitions.getPartitionIds().size()));
    };

    AsyncDocumentTree<V> tree = new PartitionedAsyncDocumentTree<>(name(), trees, partitioner);
    if (relaxedReadConsistency()) {
      tree = new CachingAsyncDocumentTree<>(tree);
    }
    return tree;
  }
}