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
package io.atomix.core.election.impl;

import com.google.common.collect.Maps;

import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.LeaderElectorBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of {@code LeaderElectorBuilder}.
 */
public class LeaderElectorProxyBuilder<T> extends LeaderElectorBuilder<T> {
  private final PrimitiveManagementService managementService;

  public LeaderElectorProxyBuilder(String name, PrimitiveManagementService managementService) {
    super(name);
    this.managementService = checkNotNull(managementService);
  }

  private CompletableFuture<AsyncLeaderElector<T>> newLeaderElector(PrimitiveProxy proxy) {
    return proxy.connect()
        .thenApply(p -> new TranscodingAsyncLeaderElector<T, byte[]>(
            new LeaderElectorProxy(proxy), serializer()::encode, serializer()::decode));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<LeaderElector<T>> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    PartitionGroup partitions = managementService.getPartitionService().getPartitionGroup(protocol);

    Map<PartitionId, CompletableFuture<AsyncLeaderElector<T>>> electors = Maps.newConcurrentMap();
    for (Partition partition : partitions.getPartitions()) {
      electors.put(partition.id(),
          newLeaderElector(partition.getPrimitiveClient().newProxy(name(), primitiveType(), protocol)));
    }

    Partitioner<String> partitioner = topic -> partitions.getPartition(topic).id();
    return Futures.allOf(new ArrayList<>(electors.values()))
        .thenApply(e -> new PartitionedAsyncLeaderElector<T>(name(), Maps.transformValues(electors, v -> v.getNow(null)), partitioner).sync());
  }
}
