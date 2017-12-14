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
package io.atomix.protocols.raft.partition;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.primitive.PrimitiveProtocol.Type;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft partition group.
 */
public class RaftPartitionGroup implements ManagedPartitionGroup {

  /**
   * Returns a new Raft partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftPartitionGroup.class);

  private final String name;
  private final int partitionSize;
  private final Map<PartitionId, RaftPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private final ClusterEventListener clusterEventListener = this::handleClusterEvent;
  private PartitionManagementService managementService;
  private Collection<PartitionMetadata> metadata;
  private CompletableFuture<Void> metadataChangeFuture = CompletableFuture.completedFuture(null);

  public RaftPartitionGroup(String name, Collection<RaftPartition> partitions, int partitionSize) {
    this.name = name;
    this.partitionSize = partitionSize;
    partitions.forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitionIds.add(p.id());
    });
    Collections.sort(sortedPartitionIds);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type type() {
    return RaftProtocol.TYPE;
  }

  @Override
  public Partition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) partitions.values();
  }

  @Override
  public List<PartitionId> getPartitionIds() {
    return sortedPartitionIds;
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> open(PartitionManagementService managementService) {
    this.managementService = managementService;
    managementService.getClusterService().addListener(clusterEventListener);
    this.metadata = buildPartitions(managementService.getClusterService());
    List<CompletableFuture<Partition>> futures = metadata.stream()
        .map(metadata -> {
          RaftPartition partition = partitions.get(metadata.id());
          return partition.open(metadata, managementService);
        })
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  private synchronized void handleClusterEvent(ClusterEvent event) {
    if (event.type() == ClusterEvent.Type.NODE_ADDED && event.subject().type() == Node.Type.DATA) {
      metadataChangeFuture = metadataChangeFuture.thenCompose(v -> {
        Collection<PartitionMetadata> partitions = buildPartitions(managementService.getClusterService());
        if (!this.metadata.equals(partitions)) {
          this.metadata = partitions;
          return Futures.allOf(partitions.stream().map(partitionMetadata -> {
            RaftPartition partition = this.partitions.get(partitionMetadata.id());
            return partition.update(partitionMetadata, managementService);
          }).collect(Collectors.toList())).thenApply(l -> null);
        }
        return CompletableFuture.completedFuture(null);
      });
    }
  }

  private Collection<PartitionMetadata> buildPartitions(ClusterService clusterService) {
    int partitionSize = this.partitionSize;
    if (partitionSize == 0) {
      partitionSize = clusterService.getNodes().size();
    }

    List<NodeId> sorted = new ArrayList<>(clusterService.getNodes().stream()
        .filter(node -> node.type() == Node.Type.DATA)
        .map(Node::id)
        .collect(Collectors.toSet()));
    Collections.sort(sorted);

    int length = sorted.size();
    int count = Math.min(partitionSize, length);

    Set<PartitionMetadata> metadata = Sets.newHashSet();
    for (int i = 0; i < partitions.size(); i++) {
      PartitionId partitionId = sortedPartitionIds.get(i);
      Set<NodeId> set = new HashSet<>(count);
      for (int j = 0; j < count; j++) {
        set.add(sorted.get((i + j) % length));
      }
      metadata.add(new PartitionMetadata(partitionId, set));
    }
    return metadata;
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(RaftPartition::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      LOGGER.info("Stopped");
    });
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("partitions", partitions)
        .toString();
  }

  /**
   * Raft partition group builder.
   */
  public static class Builder extends PartitionGroup.Builder {
    private int numPartitions;
    private int partitionSize;
    private StorageLevel storageLevel = StorageLevel.MAPPED;
    private File dataDirectory = new File(System.getProperty("user.dir"), "data");

    protected Builder(String name) {
      super(name);
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
     * Sets the storage level.
     *
     * @param storageLevel the storage level
     * @return the Raft partition group builder
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
      return this;
    }

    /**
     * Sets the path to the data directory.
     *
     * @param dataDir the path to the replica's data directory
     * @return the replica builder
     */
    public Builder withDataDirectory(File dataDir) {
      this.dataDirectory = checkNotNull(dataDir, "dataDir cannot be null");
      return this;
    }

    @Override
    public ManagedPartitionGroup build() {
      File partitionsDir = new File(dataDirectory, "partitions");
      List<RaftPartition> partitions = new ArrayList<>(numPartitions);
      for (int i = 0; i < numPartitions; i++) {
        partitions.add(new RaftPartition(PartitionId.from(name, i + 1), storageLevel, new File(partitionsDir, String.valueOf(i + 1))));
      }
      return new RaftPartitionGroup(name, partitions, partitionSize);
    }
  }
}
