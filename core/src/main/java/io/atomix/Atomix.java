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
package io.atomix;

import io.atomix.cluster.Cluster;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ManagedCluster;
import io.atomix.cluster.messaging.ClusterCommunicator;
import io.atomix.cluster.messaging.ManagedClusterCommunicator;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.partition.ManagedPartition;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionId;
import io.atomix.partition.impl.BasePartition;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.PrimitiveProvider;
import io.atomix.primitives.counter.AtomicCounterBuilder;
import io.atomix.primitives.counter.impl.DefaultAtomicCounterBuilder;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;
import io.atomix.primitives.generator.impl.DefaultAtomicIdGeneratorBuilder;
import io.atomix.primitives.impl.FederatedDistributedPrimitiveCreator;
import io.atomix.primitives.leadership.LeaderElectorBuilder;
import io.atomix.primitives.leadership.impl.DefaultLeaderElectorBuilder;
import io.atomix.primitives.lock.DistributedLockBuilder;
import io.atomix.primitives.lock.impl.DefaultDistributedLockBuilder;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.map.ConsistentTreeMapBuilder;
import io.atomix.primitives.map.impl.DefaultAtomicCounterMapBuilder;
import io.atomix.primitives.map.impl.DefaultConsistentMapBuilder;
import io.atomix.primitives.map.impl.DefaultConsistentTreeMapBuilder;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;
import io.atomix.primitives.multimap.impl.DefaultConsistentMultimapBuilder;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.primitives.set.impl.DefaultDistributedSetBuilder;
import io.atomix.primitives.tree.DocumentTreeBuilder;
import io.atomix.primitives.tree.impl.DefaultDocumentTreeBuilder;
import io.atomix.primitives.value.AtomicValueBuilder;
import io.atomix.primitives.value.impl.DefaultAtomicValueBuilder;
import io.atomix.utils.Managed;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix!
 */
public abstract class Atomix implements PrimitiveProvider, Managed<Atomix> {
  private final ManagedCluster cluster;
  private final ManagedMessagingService messagingService;
  private final ManagedClusterCommunicator clusterCommunicator;
  private final TreeMap<PartitionId, ManagedPartition> partitions = new TreeMap<>();
  private final DistributedPrimitiveCreator federatedPrimitiveCreator;
  private final AtomicBoolean open = new AtomicBoolean();

  protected Atomix(ManagedCluster cluster, ManagedMessagingService messagingService, ManagedClusterCommunicator clusterCommunicator, Collection<BasePartition> partitions) {
    this.cluster = checkNotNull(cluster, "cluster cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    partitions.forEach(p -> this.partitions.put(p.getId(), p));

    Map<PartitionId, DistributedPrimitiveCreator> partitionPrimitiveCreators = new HashMap<>();
    partitions.forEach(p -> partitionPrimitiveCreators.put(p.getId(), p.getPrimitiveCreator()));
    federatedPrimitiveCreator = new FederatedDistributedPrimitiveCreator(partitionPrimitiveCreators, cluster.metadata().buckets());
  }

  /**
   * Returns the Atomix cluster.
   *
   * @return the Atomix cluster
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the cluster communicator.
   *
   * @return the cluster communicator
   */
  public ClusterCommunicator communicator() {
    return clusterCommunicator;
  }

  /**
   * Returns the cluster messenger.
   *
   * @return the cluster messenger
   */
  public MessagingService messenger() {
    return messagingService;
  }

  /**
   * Returns the collection of partitions.
   *
   * @return the collection of partitions
   */
  @SuppressWarnings("unchecked")
  public Collection<Partition> partitions() {
    return (Collection) partitions.values();
  }

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition identifier
   * @return the partition or {@code null} if no partition with the given ID exists
   */
  public Partition partition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public <K, V> ConsistentMapBuilder<K, V> newConsistentMapBuilder() {
    return new DefaultConsistentMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <V> DocumentTreeBuilder<V> newDocumentTreeBuilder() {
    return new DefaultDocumentTreeBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <V> ConsistentTreeMapBuilder<V> newConsistentTreeMapBuilder() {
    return new DefaultConsistentTreeMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K, V> ConsistentMultimapBuilder<K, V> newConsistentMultimapBuilder() {
    return new DefaultConsistentMultimapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <K> AtomicCounterMapBuilder<K> newAtomicCounterMapBuilder() {
    return new DefaultAtomicCounterMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <E> DistributedSetBuilder<E> newSetBuilder() {
    return new DefaultDistributedSetBuilder<>(() -> newConsistentMapBuilder());
  }

  @Override
  public AtomicCounterBuilder newAtomicCounterBuilder() {
    return new DefaultAtomicCounterBuilder(federatedPrimitiveCreator);
  }

  @Override
  public AtomicIdGeneratorBuilder newAtomicIdGeneratorBuilder() {
    return new DefaultAtomicIdGeneratorBuilder(federatedPrimitiveCreator);
  }

  @Override
  public <V> AtomicValueBuilder<V> newAtomicValueBuilder() {
    return new DefaultAtomicValueBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <T> LeaderElectorBuilder<T> newLeaderElectorBuilder() {
    return new DefaultLeaderElectorBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public DistributedLockBuilder newLockBuilder() {
    return new DefaultDistributedLockBuilder(federatedPrimitiveCreator);
  }

  @Override
  public CompletableFuture<Atomix> open() {
    return messagingService.open()
        .thenCompose(v -> cluster.open())
        .thenCompose(v -> clusterCommunicator.open())
        .thenCompose(v -> {
          List<CompletableFuture<Partition>> futures = partitions.values().stream()
              .map(p -> p.open())
              .collect(Collectors.toList());
          return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        }).thenApply(v -> {
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
    return null;
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitions", partitions())
        .toString();
  }

  /**
   * Atomix builder.
   */
  public abstract static class Builder implements io.atomix.utils.Builder<Atomix> {
    protected ClusterMetadata clusterMetadata;

    /**
     * Sets the cluster metadata.
     *
     * @param clusterMetadata the cluster metadata
     * @return the Atomix builder
     * @throws NullPointerException if the cluster metadata is null
     */
    public Builder withClusterMetadata(ClusterMetadata clusterMetadata) {
      this.clusterMetadata = checkNotNull(clusterMetadata, "clusterMetadata cannot be null");
      return this;
    }
  }
}
