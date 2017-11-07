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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicator;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionId;
import io.atomix.partition.PartitionInfo;
import io.atomix.partition.impl.AbstractPartition;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix!
 */
public abstract class Atomix implements PrimitiveProvider {
  private final TreeMap<PartitionId, AbstractPartition> partitions = new TreeMap<>();
  private final DistributedPrimitiveCreator federatedPrimitiveCreator;

  protected Atomix(Collection<AbstractPartition> partitions, int numBuckets) {
    partitions.forEach(p -> this.partitions.put(p.getId(), p));
    federatedPrimitiveCreator = new FederatedDistributedPrimitiveCreator(
        Maps.transformValues(this.partitions, v -> v.getPrimitiveCreator()),
        numBuckets);
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
    return new DefaultDocumentTreeBuilder<V>(federatedPrimitiveCreator);
  }

  @Override
  public <V> ConsistentTreeMapBuilder<V> newConsistentTreeMapBuilder() {
    return new DefaultConsistentTreeMapBuilder<V>(federatedPrimitiveCreator);
  }

  @Override
  public <K, V> ConsistentMultimapBuilder<K, V> newConsistentMultimapBuilder() {
    return new DefaultConsistentMultimapBuilder<K, V>(federatedPrimitiveCreator);
  }

  @Override
  public <K> AtomicCounterMapBuilder<K> newAtomicCounterMapBuilder() {
    return new DefaultAtomicCounterMapBuilder<>(federatedPrimitiveCreator);
  }

  @Override
  public <E> DistributedSetBuilder<E> newSetBuilder() {
    return new DefaultDistributedSetBuilder<>(() -> this.<E, Boolean>newConsistentMapBuilder());
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
  public String toString() {
    return toStringHelper(this)
        .add("partitions", partitions())
        .toString();
  }

  /**
   * Atomix builder.
   */
  public abstract static class Builder implements io.atomix.utils.Builder<Atomix> {
    private static final int DEFAULT_PARTITION_SIZE = 3;
    private static final int DEFAULT_NUM_BUCKETS = 128;

    protected NodeId nodeId;
    protected Collection<NodeId> nodes;
    protected int numPartitions;
    protected int partitionSize = DEFAULT_PARTITION_SIZE;
    protected int numBuckets = DEFAULT_NUM_BUCKETS;
    protected ClusterCommunicator clusterCommunicator;

    /**
     * Sets the local node identifier.
     *
     * @param nodeId the local node identifier
     * @return the Atomix builder
     */
    public Builder withNodeId(NodeId nodeId) {
      this.nodeId = checkNotNull(nodeId, "nodeId cannot be null");
      return this;
    }

    /**
     * Sets the set of nodes in the cluster.
     *
     * @param nodes the set of nodes in the cluster
     * @return the Atomix builder
     */
    public Builder withNodes(NodeId... nodes) {
      return withNodes(Arrays.asList(nodes));
    }

    /**
     * Sets the set of nodes in the cluster.
     *
     * @param nodes the set of nodes in the cluster
     * @return the Atomix builder
     */
    public Builder withNodes(Collection<NodeId> nodes) {
      this.nodes = checkNotNull(nodes, "nodes cannot be null");
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the Atomix builder
     */
    public Builder withNumPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    /**
     * Sets the cluster communicator.
     *
     * @param clusterCommunicator the cluster communicator
     * @return the Atomix builder
     */
    public Builder withClusterCommunicator(ClusterCommunicator clusterCommunicator) {
      this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
      return this;
    }

    static Set<PartitionInfo> buildPartitionInfo(Collection<NodeId> nodes, int numPartitions, int partitionSize) {
      List<NodeId> sorted = new ArrayList<>(nodes);
      Collections.sort(sorted);

      Set<PartitionInfo> partitions = Sets.newHashSet();
      for (int i = 0; i < numPartitions; i++) {
        Set<NodeId> set = new HashSet<>(partitionSize);
        for (int j = 0; j < partitionSize; j++) {
          set.add(sorted.get((i + j) % numPartitions));
        }
        partitions.add(new PartitionInfo(PartitionId.from((i + 1)), set));
      }
      return partitions;
    }
  }
}
