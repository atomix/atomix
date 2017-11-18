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
package io.atomix.election.impl;

import com.google.common.collect.Maps;
import io.atomix.election.AsyncLeaderElector;
import io.atomix.election.LeaderElectorBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.proxy.PrimitiveProxy;

import java.util.Map;

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

  private AsyncLeaderElector<T> newLeaderElector(PrimitiveProxy proxy) {
    AsyncLeaderElector<byte[]> leaderElector = new LeaderElectorProxy(proxy.open().join());
    return new TranscodingAsyncLeaderElector<>(leaderElector, serializer()::encode, serializer()::decode);
  }

  @Override
  @SuppressWarnings("unchecked")
  public AsyncLeaderElector<T> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    PartitionGroup partitions = managementService.getPartitionService().getPartitionGroup(protocol);

    Map<PartitionId, AsyncLeaderElector<T>> electors = Maps.newConcurrentMap();
    for (Partition partition : partitions.getPartitions()) {
      electors.put(partition.id(),
          newLeaderElector(partition.getPrimitiveClient().proxyBuilder(name(), primitiveType(), protocol).build()));
    }

    Partitioner<String> partitioner = topic -> partitions.getPartition(topic).id();
    return new PartitionedAsyncLeaderElector(name(), electors, partitioner);
  }
}
