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
package io.atomix.protocols.backup.partition;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveProtocol.Type;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadPoolContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Primary-backup partition group.
 */
public class PrimaryBackupPartitionGroup implements ManagedPartitionGroup {

  /**
   * Returns a new primary-backup partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryBackupPartitionGroup.class);

  private final String name;
  private final Map<PartitionId, PrimaryBackupPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private ThreadContextFactory threadFactory;

  public PrimaryBackupPartitionGroup(String name, Collection<PrimaryBackupPartition> partitions) {
    this.name = name;
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
    return MultiPrimaryProtocol.TYPE;
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
    threadFactory = new ThreadPoolContextFactory("atomix-" + name() + "-%d", Runtime.getRuntime().availableProcessors() * 2, LOGGER);
    List<CompletableFuture<Partition>> futures = partitions.values().stream()
        .map(p -> p.open(managementService, threadFactory))
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(PrimaryBackupPartition::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      threadFactory.close();
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
   * Primary-backup partition group builder.
   */
  public static class Builder extends PartitionGroup.Builder {
    private int numPartitions;

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

    @Override
    public ManagedPartitionGroup build() {
      List<PrimaryBackupPartition> partitions = new ArrayList<>(numPartitions);
      for (int i = 0; i < numPartitions; i++) {
        partitions.add(new PrimaryBackupPartition(PartitionId.from(name, i + 1)));
      }
      return new PrimaryBackupPartitionGroup(name, partitions);
    }
  }
}
