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
package io.atomix.protocols.backup.partition.impl;

import com.google.common.collect.ImmutableMap;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.counter.AtomicCounterBuilder;
import io.atomix.counter.impl.AtomicCounterService;
import io.atomix.counter.impl.DefaultAtomicCounterBuilder;
import io.atomix.generator.AtomicIdGeneratorBuilder;
import io.atomix.generator.impl.DefaultAtomicIdGeneratorBuilder;
import io.atomix.leadership.LeaderElectorBuilder;
import io.atomix.leadership.impl.DefaultLeaderElectorBuilder;
import io.atomix.leadership.impl.LeaderElectorService;
import io.atomix.lock.DistributedLockBuilder;
import io.atomix.lock.impl.DefaultDistributedLockBuilder;
import io.atomix.lock.impl.DistributedLockService;
import io.atomix.map.AtomicCounterMapBuilder;
import io.atomix.map.ConsistentMapBuilder;
import io.atomix.map.ConsistentTreeMapBuilder;
import io.atomix.map.impl.AtomicCounterMapService;
import io.atomix.map.impl.ConsistentMapService;
import io.atomix.map.impl.ConsistentTreeMapService;
import io.atomix.map.impl.DefaultAtomicCounterMapBuilder;
import io.atomix.map.impl.DefaultConsistentMapBuilder;
import io.atomix.map.impl.DefaultConsistentTreeMapBuilder;
import io.atomix.multimap.ConsistentMultimapBuilder;
import io.atomix.multimap.impl.ConsistentSetMultimapService;
import io.atomix.multimap.impl.DefaultConsistentMultimapBuilder;
import io.atomix.primitive.DistributedPrimitiveCreator;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypes;
import io.atomix.primitive.partition.ManagedPartition;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.queue.WorkQueueBuilder;
import io.atomix.queue.impl.DefaultWorkQueueBuilder;
import io.atomix.queue.impl.WorkQueueService;
import io.atomix.set.DistributedSetBuilder;
import io.atomix.set.impl.DefaultDistributedSetBuilder;
import io.atomix.tree.DocumentTreeBuilder;
import io.atomix.tree.impl.DefaultDocumentTreeBuilder;
import io.atomix.tree.impl.DocumentTreeService;
import io.atomix.value.AtomicValueBuilder;
import io.atomix.value.impl.AtomicValueService;
import io.atomix.value.impl.DefaultAtomicValueBuilder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Primary-backup partition.
 */
public class PrimaryBackupPartition implements ManagedPartition {
  private final PartitionId partitionId;
  private final PrimaryBackupPartitionServer server;
  private final PrimaryBackupPartitionClient client;
  private final AtomicBoolean open = new AtomicBoolean();

  public static final Map<String, Supplier<PrimitiveService>> SERVICES =
      ImmutableMap.<String, Supplier<PrimitiveService>>builder()
          .put(PrimitiveTypes.CONSISTENT_MAP.name(), ConsistentMapService::new)
          .put(PrimitiveTypes.CONSISTENT_TREEMAP.name(), ConsistentTreeMapService::new)
          .put(PrimitiveTypes.CONSISTENT_MULTIMAP.name(), ConsistentSetMultimapService::new)
          .put(PrimitiveTypes.COUNTER_MAP.name(), AtomicCounterMapService::new)
          .put(PrimitiveTypes.COUNTER.name(), AtomicCounterService::new)
          .put(PrimitiveTypes.LEADER_ELECTOR.name(), LeaderElectorService::new)
          .put(PrimitiveTypes.LOCK.name(), DistributedLockService::new)
          .put(PrimitiveTypes.WORK_QUEUE.name(), WorkQueueService::new)
          .put(PrimitiveTypes.VALUE.name(), AtomicValueService::new)
          .put(PrimitiveTypes.DOCUMENT_TREE.name(),
              () -> new DocumentTreeService(Ordering.NATURAL))
          .put(String.format("%s-%s", PrimitiveTypes.DOCUMENT_TREE.name(), Ordering.NATURAL),
              () -> new DocumentTreeService(Ordering.NATURAL))
          .put(String.format("%s-%s", PrimitiveTypes.DOCUMENT_TREE.name(), Ordering.INSERTION),
              () -> new DocumentTreeService(Ordering.INSERTION))
          .build();

  public PrimaryBackupPartition(
      PartitionId partitionId,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ReplicaInfoProvider replicaProvider) {
    this.partitionId = partitionId;
    this.server = new PrimaryBackupPartitionServer(this, clusterService, communicationService, replicaProvider);
    this.client = new PrimaryBackupPartitionClient(this, clusterService, communicationService, replicaProvider);
  }

  @Override
  public PartitionId id() {
    return partitionId;
  }

  /**
   * Returns the partition name.
   *
   * @return the partition name
   */
  public String name() {
    return String.format("partition-%d", partitionId.id());
  }

  @Override
  public DistributedPrimitiveCreator getPrimitiveCreator() {
    return client;
  }

  @Override
  public <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder() {
    return new DefaultConsistentMapBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <V> DocumentTreeBuilder<V> documentTreeBuilder() {
    return new DefaultDocumentTreeBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder() {
    return new DefaultConsistentTreeMapBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder() {
    return new DefaultConsistentMultimapBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder() {
    return new DefaultAtomicCounterMapBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <E> DistributedSetBuilder<E> setBuilder() {
    return new DefaultDistributedSetBuilder<>(() -> consistentMapBuilder());
  }

  @Override
  public AtomicCounterBuilder atomicCounterBuilder() {
    return new DefaultAtomicCounterBuilder(getPrimitiveCreator());
  }

  @Override
  public AtomicIdGeneratorBuilder atomicIdGeneratorBuilder() {
    return new DefaultAtomicIdGeneratorBuilder(getPrimitiveCreator());
  }

  @Override
  public <V> AtomicValueBuilder<V> atomicValueBuilder() {
    return new DefaultAtomicValueBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <T> LeaderElectorBuilder<T> leaderElectorBuilder() {
    return new DefaultLeaderElectorBuilder<>(getPrimitiveCreator());
  }

  @Override
  public <E> WorkQueueBuilder<E> workQueueBuilder() {
    return new DefaultWorkQueueBuilder<>(getPrimitiveCreator());
  }

  @Override
  public DistributedLockBuilder lockBuilder() {
    return new DefaultDistributedLockBuilder(getPrimitiveCreator());
  }

  @Override
  public Set<String> getPrimitiveNames(PrimitiveType primitiveType) {
    return getPrimitiveCreator().getPrimitiveNames(primitiveType);
  }

  @Override
  public CompletableFuture<Partition> open() {
    return server.open().thenCompose(v -> client.open()).thenApply(v -> {
      open.set(true);
      return this;
    });
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    client.close().whenComplete((result, error) -> {
      server.close().whenComplete((serverResult, serverError) -> {
        open.set(false);
        future.complete(null);
      });
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
