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
package io.atomix.partition.impl;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.partition.ManagedPartition;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionId;
import io.atomix.partition.PartitionMetadata;
import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.Ordering;
import io.atomix.primitives.counter.AtomicCounterBuilder;
import io.atomix.primitives.counter.impl.DefaultAtomicCounterBuilder;
import io.atomix.primitives.counter.impl.RaftAtomicCounterService;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;
import io.atomix.primitives.generator.impl.DefaultAtomicIdGeneratorBuilder;
import io.atomix.primitives.leadership.LeaderElectorBuilder;
import io.atomix.primitives.leadership.impl.DefaultLeaderElectorBuilder;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorService;
import io.atomix.primitives.lock.DistributedLockBuilder;
import io.atomix.primitives.lock.impl.DefaultDistributedLockBuilder;
import io.atomix.primitives.lock.impl.RaftDistributedLockService;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.map.ConsistentTreeMapBuilder;
import io.atomix.primitives.map.impl.DefaultAtomicCounterMapBuilder;
import io.atomix.primitives.map.impl.DefaultConsistentMapBuilder;
import io.atomix.primitives.map.impl.DefaultConsistentTreeMapBuilder;
import io.atomix.primitives.map.impl.RaftAtomicCounterMapService;
import io.atomix.primitives.map.impl.RaftConsistentMapService;
import io.atomix.primitives.map.impl.RaftConsistentTreeMapService;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;
import io.atomix.primitives.multimap.impl.DefaultConsistentMultimapBuilder;
import io.atomix.primitives.multimap.impl.RaftConsistentSetMultimapService;
import io.atomix.primitives.queue.WorkQueueBuilder;
import io.atomix.primitives.queue.impl.DefaultWorkQueueBuilder;
import io.atomix.primitives.queue.impl.RaftWorkQueueService;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.primitives.set.impl.DefaultDistributedSetBuilder;
import io.atomix.primitives.tree.DocumentTreeBuilder;
import io.atomix.primitives.tree.impl.DefaultDocumentTreeBuilder;
import io.atomix.primitives.tree.impl.RaftDocumentTreeService;
import io.atomix.primitives.value.AtomicValueBuilder;
import io.atomix.primitives.value.impl.DefaultAtomicValueBuilder;
import io.atomix.primitives.value.impl.RaftAtomicValueService;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.service.RaftService;
import io.atomix.serializer.Serializer;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Abstract partition.
 */
public class RaftPartition implements ManagedPartition {
  protected final AtomicBoolean isOpened = new AtomicBoolean(false);
  protected final ClusterCommunicationService clusterCommunicator;
  protected final PartitionMetadata partition;
  protected final NodeId localNodeId;
  private final File dataDir;
  private final RaftPartitionClient client;
  private final RaftPartitionServer server;

  public static final Map<String, Supplier<RaftService>> RAFT_SERVICES =
      ImmutableMap.<String, Supplier<RaftService>>builder()
          .put(DistributedPrimitive.Type.CONSISTENT_MAP.name(), RaftConsistentMapService::new)
          .put(DistributedPrimitive.Type.CONSISTENT_TREEMAP.name(), RaftConsistentTreeMapService::new)
          .put(DistributedPrimitive.Type.CONSISTENT_MULTIMAP.name(), RaftConsistentSetMultimapService::new)
          .put(DistributedPrimitive.Type.COUNTER_MAP.name(), RaftAtomicCounterMapService::new)
          .put(DistributedPrimitive.Type.COUNTER.name(), RaftAtomicCounterService::new)
          .put(DistributedPrimitive.Type.LEADER_ELECTOR.name(), RaftLeaderElectorService::new)
          .put(DistributedPrimitive.Type.LOCK.name(), RaftDistributedLockService::new)
          .put(DistributedPrimitive.Type.WORK_QUEUE.name(), RaftWorkQueueService::new)
          .put(DistributedPrimitive.Type.VALUE.name(), RaftAtomicValueService::new)
          .put(DistributedPrimitive.Type.DOCUMENT_TREE.name(),
              () -> new RaftDocumentTreeService(Ordering.NATURAL))
          .put(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), Ordering.NATURAL),
              () -> new RaftDocumentTreeService(Ordering.NATURAL))
          .put(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), Ordering.INSERTION),
              () -> new RaftDocumentTreeService(Ordering.INSERTION))
          .build();

  public RaftPartition(
      NodeId nodeId,
      PartitionMetadata partition,
      ClusterCommunicationService clusterCommunicator,
      File dataDir) {
    this.localNodeId = nodeId;
    this.partition = partition;
    this.clusterCommunicator = clusterCommunicator;
    this.dataDir = dataDir;
    this.client = createClient();
    this.server = createServer();
  }

  @Override
  public PartitionId id() {
    return partition.id();
  }

  /**
   * Returns the partition primitive creator.
   *
   * @return the partition primitive creator
   */
  public DistributedPrimitiveCreator getPrimitiveCreator() {
    return client;
  }

  /**
   * Returns the partition name.
   *
   * @return the partition name
   */
  public String name() {
    return String.format("partition-%d", partition.id().id());
  }

  /**
   * Returns the identifiers of partition members.
   *
   * @return partition member instance ids
   */
  Collection<NodeId> members() {
    return partition.members();
  }

  /**
   * Returns the {@link MemberId identifiers} of partition members.
   *
   * @return partition member identifiers
   */
  Collection<MemberId> getMemberIds() {
    return Collections2.transform(members(), n -> MemberId.from(n.id()));
  }

  /**
   * Returns the partition data directory.
   *
   * @return the partition data directory
   */
  File getDataDir() {
    return dataDir;
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
  public Set<String> getPrimitiveNames(DistributedPrimitive.Type primitiveType) {
    return getPrimitiveCreator().getPrimitiveNames(primitiveType);
  }

  @Override
  public CompletableFuture<Partition> open() {
    if (partition.members().contains(localNodeId)) {
      return server.open()
          .thenCompose(v -> client.open())
          .thenAccept(v -> isOpened.set(true))
          .thenApply(v -> null);
    }
    return client.open()
        .thenAccept(v -> isOpened.set(true))
        .thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return isOpened.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    // We do not explicitly close the server and instead let the cluster
    // deal with this as an unclean exit.
    return client.close();
  }

  @Override
  public boolean isClosed() {
    return !isOpened.get();
  }

  /**
   * Creates a Raft server.
   */
  protected RaftPartitionServer createServer() {
    return new RaftPartitionServer(
        this,
        MemberId.from(localNodeId.id()),
        clusterCommunicator);
  }

  /**
   * Creates a Raft client.
   */
  private RaftPartitionClient createClient() {
    return new RaftPartitionClient(
        this,
        MemberId.from(localNodeId.id()),
        new RaftClientCommunicator(
            name(),
            Serializer.using(RaftNamespaces.RAFT_PROTOCOL),
            clusterCommunicator));
  }

  /**
   * Deletes the partition.
   *
   * @return future to be completed once the partition has been deleted
   */
  public CompletableFuture<Void> delete() {
    return server.close().thenCompose(v -> client.close()).thenRun(() -> server.delete());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionId", id())
        .toString();
  }
}
