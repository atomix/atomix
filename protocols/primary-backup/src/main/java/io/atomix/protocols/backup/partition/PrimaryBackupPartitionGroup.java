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
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.MemberGroup;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol.Type;
import io.atomix.protocols.backup.PrimaryBackupProtocol;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadPoolContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

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
    return new Builder(new PrimaryBackupPartitionGroupConfig().setName(name));
  }

  private static Collection<PrimaryBackupPartition> buildPartitions(PrimaryBackupPartitionGroupConfig config) {
    List<PrimaryBackupPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new PrimaryBackupPartition(PartitionId.from(config.getName(), i + 1), config.getMemberGroupProvider()));
    }
    return partitions;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryBackupPartitionGroup.class);

  private final String name;
  private final PrimaryBackupPartitionGroupConfig config;
  private final Map<PartitionId, PrimaryBackupPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private ThreadContextFactory threadFactory;

  public PrimaryBackupPartitionGroup(PrimaryBackupPartitionGroupConfig config) {
    this.config = config;
    this.name = checkNotNull(config.getName());
    buildPartitions(config).forEach(p -> {
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
    return PrimaryBackupProtocol.TYPE;
  }

  @Override
  public PartitionGroupConfig config() {
    return config;
  }

  @Override
  public PrimitiveProtocol newProtocol() {
    return PrimaryBackupProtocol.builder(name)
        .withRecovery(Recovery.RECOVER)
        .withBackups(2)
        .withReplication(Replication.SYNCHRONOUS)
        .build();
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
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    threadFactory = new ThreadPoolContextFactory("atomix-" + name() + "-%d", Runtime.getRuntime().availableProcessors() * 2, LOGGER);
    List<CompletableFuture<Partition>> futures = partitions.values().stream()
        .map(p -> p.join(managementService, threadFactory))
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    threadFactory = new ThreadPoolContextFactory("atomix-" + name() + "-%d", Runtime.getRuntime().availableProcessors() * 2, LOGGER);
    List<CompletableFuture<Partition>> futures = partitions.values().stream()
        .map(p -> p.connect(managementService, threadFactory))
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
      ThreadContextFactory threadFactory = this.threadFactory;
      if (threadFactory != null) {
        threadFactory.close();
      }
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
  public static class Builder extends PartitionGroup.Builder<PrimaryBackupPartitionGroupConfig> {
    protected Builder(PrimaryBackupPartitionGroupConfig config) {
      super(config);
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the partition group builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    /**
     * Sets the member group provider.
     *
     * @param memberGroupProvider the member group provider
     * @return the partition group builder
     */
    public Builder withMemberGroupProvider(MemberGroupProvider memberGroupProvider) {
      config.setMemberGroupProvider(memberGroupProvider);
      return this;
    }

    /**
     * Sets the member group strategy.
     *
     * @param memberGroupStrategy the member group strategy
     * @return the partition group builder
     */
    public Builder withMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
      return withMemberGroupProvider(memberGroupStrategy);
    }

    /**
     * Sets the member groups.
     *
     * @param memberGroups the member groups
     * @return the partition group builder
     */
    public Builder withMemberGroups(MemberGroup... memberGroups) {
      return withMemberGroups(Arrays.asList(memberGroups));
    }

    /**
     * Sets the member groups.
     *
     * @param memberGroups the member groups
     * @return the partition group builder
     */
    public Builder withMemberGroups(Collection<MemberGroup> memberGroups) {
      return withMemberGroupProvider(nodes -> memberGroups);
    }

    @Override
    public PrimaryBackupPartitionGroup build() {
      return new PrimaryBackupPartitionGroup(config);
    }
  }
}
