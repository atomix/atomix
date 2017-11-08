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

import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ManagedClusterCommunicationService;
import io.atomix.cluster.messaging.impl.DefaultClusterCommunicationService;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.netty.NettyMessagingService;
import io.atomix.partition.ManagedPartitionService;
import io.atomix.partition.PartitionMetadata;
import io.atomix.partition.PartitionService;
import io.atomix.partition.impl.DefaultPartitionService;
import io.atomix.partition.impl.RaftPartition;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.PrimitiveService;
import io.atomix.primitives.counter.AtomicCounterBuilder;
import io.atomix.primitives.generator.AtomicIdGeneratorBuilder;
import io.atomix.primitives.impl.FederatedPrimitiveService;
import io.atomix.primitives.leadership.LeaderElectorBuilder;
import io.atomix.primitives.lock.DistributedLockBuilder;
import io.atomix.primitives.map.AtomicCounterMapBuilder;
import io.atomix.primitives.map.ConsistentMapBuilder;
import io.atomix.primitives.map.ConsistentTreeMapBuilder;
import io.atomix.primitives.multimap.ConsistentMultimapBuilder;
import io.atomix.primitives.set.DistributedSetBuilder;
import io.atomix.primitives.tree.DocumentTreeBuilder;
import io.atomix.primitives.value.AtomicValueBuilder;
import io.atomix.utils.Managed;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix!
 */
public abstract class Atomix implements PrimitiveService, Managed<Atomix> {
  private final ManagedClusterService cluster;
  private final ManagedMessagingService messagingService;
  private final ManagedClusterCommunicationService clusterCommunicator;
  private final ManagedPartitionService partitions;
  private final PrimitiveService primitives;
  private final AtomicBoolean open = new AtomicBoolean();

  protected Atomix(
      AtomixMetadata metadata,
      ManagedClusterService cluster,
      ManagedMessagingService messagingService,
      ManagedClusterCommunicationService clusterCommunicator,
      ManagedPartitionService partitions,
      PrimitiveService primitives) {
    this.cluster = checkNotNull(cluster, "cluster cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    this.partitions = checkNotNull(partitions, "partitions cannot be null");
    this.primitives = checkNotNull(primitives, "primitives cannot be null");
  }

  /**
   * Returns the Atomix cluster.
   *
   * @return the Atomix cluster
   */
  public ClusterService getClusterService() {
    return cluster;
  }

  /**
   * Returns the cluster communicator.
   *
   * @return the cluster communicator
   */
  public ClusterCommunicationService getCommunicationService() {
    return clusterCommunicator;
  }

  /**
   * Returns the cluster messenger.
   *
   * @return the cluster messenger
   */
  public MessagingService getMessagingService() {
    return messagingService;
  }

  /**
   * Returns the partition service.
   *
   * @return the partition service
   */
  public PartitionService getPartitionService() {
    return partitions;
  }

  /**
   * Returns the primitive service.
   *
   * @return the primitive service
   */
  public PrimitiveService getPrimitiveService() {
    return primitives;
  }

  @Override
  public <K, V> ConsistentMapBuilder<K, V> newConsistentMapBuilder() {
    return primitives.newConsistentMapBuilder();
  }

  @Override
  public <V> DocumentTreeBuilder<V> newDocumentTreeBuilder() {
    return primitives.newDocumentTreeBuilder();
  }

  @Override
  public <V> ConsistentTreeMapBuilder<V> newConsistentTreeMapBuilder() {
    return primitives.newConsistentTreeMapBuilder();
  }

  @Override
  public <K, V> ConsistentMultimapBuilder<K, V> newConsistentMultimapBuilder() {
    return primitives.newConsistentMultimapBuilder();
  }

  @Override
  public <K> AtomicCounterMapBuilder<K> newAtomicCounterMapBuilder() {
    return primitives.newAtomicCounterMapBuilder();
  }

  @Override
  public <E> DistributedSetBuilder<E> newSetBuilder() {
    return primitives.newSetBuilder();
  }

  @Override
  public AtomicCounterBuilder newAtomicCounterBuilder() {
    return primitives.newAtomicCounterBuilder();
  }

  @Override
  public AtomicIdGeneratorBuilder newAtomicIdGeneratorBuilder() {
    return primitives.newAtomicIdGeneratorBuilder();
  }

  @Override
  public <V> AtomicValueBuilder<V> newAtomicValueBuilder() {
    return primitives.newAtomicValueBuilder();
  }

  @Override
  public <T> LeaderElectorBuilder<T> newLeaderElectorBuilder() {
    return primitives.newLeaderElectorBuilder();
  }

  @Override
  public DistributedLockBuilder newLockBuilder() {
    return primitives.newLockBuilder();
  }

  @Override
  public CompletableFuture<Atomix> open() {
    return messagingService.open()
        .thenCompose(v -> cluster.open())
        .thenCompose(v -> clusterCommunicator.open())
        .thenCompose(v -> partitions.open())
        .thenApply(v -> {
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
        .add("partitions", getPartitionService())
        .toString();
  }

  /**
   * Atomix builder.
   */
  public abstract static class Builder implements io.atomix.utils.Builder<Atomix> {
    private static final String DEFAULT_CLUSTER_NAME = "atomix";
    protected String name = DEFAULT_CLUSTER_NAME;
    protected Node localNode;
    protected Collection<Node> bootstrapNodes;
    protected int numPartitions;
    protected int partitionSize;
    protected int numBuckets;
    protected Collection<PartitionMetadata> partitions;

    /**
     * Sets the cluster name.
     *
     * @param name the cluster name
     * @return the cluster metadata builder
     * @throws NullPointerException if the name is null
     */
    public Builder withClusterName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the local node metadata.
     *
     * @param localNode the local node metadata
     * @return the cluster metadata builder
     */
    public Builder withLocalNode(Node localNode) {
      this.localNode = checkNotNull(localNode, "localNode cannot be null");
      return this;
    }

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Node... bootstrapNodes) {
      return withBootstrapNodes(Arrays.asList(checkNotNull(bootstrapNodes)));
    }

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Collection<Node> bootstrapNodes) {
      this.bootstrapNodes = checkNotNull(bootstrapNodes, "bootstrapNodes cannot be null");
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withNumPartitions(int numPartitions) {
      checkArgument(numPartitions > 0, "numPartitions must be positive");
      this.numPartitions = numPartitions;
      return this;
    }

    /**
     * Sets the partition size.
     *
     * @param partitionSize the partition size
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the partition size is not positive
     */
    public Builder withPartitionSize(int partitionSize) {
      checkArgument(partitionSize > 0, "partitionSize must be positive");
      this.partitionSize = partitionSize;
      return this;
    }

    /**
     * Sets the number of buckets within each partition.
     *
     * @param numBuckets the number of buckets within each partition
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the number of buckets within each partition is not positive
     */
    public Builder withNumBuckets(int numBuckets) {
      checkArgument(numBuckets > 0, "numBuckets must be positive");
      this.numBuckets = numBuckets;
      return this;
    }

    /**
     * Sets the partitions.
     *
     * @param partitions the partitions
     * @return the cluster metadata builder
     */
    public Builder withPartitions(Collection<PartitionMetadata> partitions) {
      this.partitions = checkNotNull(partitions, "partitions cannot be null");
      return this;
    }

    /**
     * Builds Atomix metadata.
     */
    protected AtomixMetadata buildMetadata() {
      return AtomixMetadata.newBuilder()
          .withLocalNode(localNode)
          .withBootstrapNodes(bootstrapNodes)
          .withNumPartitions(numPartitions)
          .withPartitionSize(partitionSize)
          .withNumBuckets(numBuckets)
          .withPartitions(partitions)
          .build();
    }

    /**
     * Builds a default messaging service.
     */
    protected ManagedMessagingService buildMessagingService() {
      return NettyMessagingService.newBuilder()
          .withName(name)
          .withEndpoint(new Endpoint(localNode.address(), localNode.port()))
          .build();
    }

    /**
     * Builds a cluster service.
     */
    protected ManagedClusterService buildClusterService(MessagingService messagingService) {
      return new DefaultClusterService(ClusterMetadata.newBuilder()
          .withLocalNode(localNode)
          .withBootstrapNodes(bootstrapNodes)
          .build(), messagingService);
    }

    /**
     * Builds a cluster communication service.
     */
    protected ManagedClusterCommunicationService buildClusterCommunicationService(
        ClusterService clusterService, MessagingService messagingService) {
      return new DefaultClusterCommunicationService(clusterService, messagingService);
    }

    /**
     * Builds a partition service.
     */
    protected ManagedPartitionService buildPartitionService(
        AtomixMetadata metadata, Function<PartitionMetadata, RaftPartition> factory) {
      Collection<RaftPartition> partitions = metadata.partitions().stream()
          .map(factory)
          .collect(Collectors.toList());
      return new DefaultPartitionService(partitions);
    }

    /**
     * Builds a primitive service.
     */
    protected PrimitiveService buildPrimitiveService(PartitionService partitionService) {
      Map<Integer, DistributedPrimitiveCreator > members = new HashMap<>();
      partitionService.getPartitions().forEach(p -> members.put(p.id().id(), partitionService.getPrimitiveCreator(p.id())));
      return new FederatedPrimitiveService(members, numBuckets);
    }
  }
}
