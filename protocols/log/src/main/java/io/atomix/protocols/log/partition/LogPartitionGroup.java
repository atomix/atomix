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
package io.atomix.protocols.log.partition;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.memory.MemorySize;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log partition group.
 */
public class LogPartitionGroup implements ManagedPartitionGroup {
  public static final Type TYPE = new Type();

  /**
   * Returns a new log partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(new LogPartitionGroupConfig().setName(name));
  }

  /**
   * Log partition group type.
   */
  public static class Type implements PartitionGroup.Type<LogPartitionGroupConfig> {
    private static final String NAME = "log";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Namespace namespace() {
      return Namespace.builder()
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 300)
          .register(LogPartitionGroupConfig.class)
          .register(LogStorageConfig.class)
          .register(LogCompactionConfig.class)
          .register(MemorySize.class)
          .register(StorageLevel.class)
          .build();
    }

    @Override
    public LogPartitionGroupConfig newConfig() {
      return new LogPartitionGroupConfig();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(LogPartitionGroupConfig config) {
      return new LogPartitionGroup(config);
    }
  }

  private static Collection<LogPartition> buildPartitions(LogPartitionGroupConfig config) {
    List<LogPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new LogPartition(PartitionId.from(config.getName(), i + 1), config));
    }
    return partitions;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LogPartitionGroup.class);

  private final String name;
  private final LogPartitionGroupConfig config;
  private final Map<PartitionId, LogPartition> partitions = Maps.newConcurrentMap();
  private final List<LogPartition> sortedPartitions = Lists.newCopyOnWriteArrayList();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private ThreadContextFactory threadFactory;

  public LogPartitionGroup(LogPartitionGroupConfig config) {
    this.config = config;
    this.name = checkNotNull(config.getName());
    buildPartitions(config).forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitions.add(p);
      this.sortedPartitionIds.add(p.id());
    });
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PartitionGroup.Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProtocol.Type protocol() {
    return DistributedLogProtocol.TYPE;
  }

  @Override
  public PartitionGroupConfig config() {
    return config;
  }

  @Override
  public ProxyProtocol newProtocol() {
    return DistributedLogProtocol.builder(name)
        .withRecovery(Recovery.RECOVER)
        .build();
  }

  @Override
  public LogPartition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) sortedPartitions;
  }

  @Override
  public List<PartitionId> getPartitionIds() {
    return sortedPartitionIds;
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 32), 4);
    threadFactory = new BlockingAwareThreadPoolContextFactory("atomix-" + name() + "-%d", threadPoolSize, LOGGER);
    List<CompletableFuture<Partition>> futures = partitions.values().stream()
        .map(p -> p.join(managementService, threadFactory))
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 32), 4);
    threadFactory = new BlockingAwareThreadPoolContextFactory("atomix-" + name() + "-%d", threadPoolSize, LOGGER);
    List<CompletableFuture<Partition>> futures = partitions.values().stream()
        .map(p -> p.connect(managementService, threadFactory))
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(LogPartition::close)
        .collect(Collectors.toList());
    // Shutdown ThreadContextFactory on FJP common thread
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).whenCompleteAsync((r, e) -> {
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
   * Log partition group builder.
   */
  public static class Builder extends PartitionGroup.Builder<LogPartitionGroupConfig> {
    protected Builder(LogPartitionGroupConfig config) {
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
     * Sets the member group strategy.
     *
     * @param memberGroupStrategy the member group strategy
     * @return the partition group builder
     */
    public Builder withMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
      config.setMemberGroupStrategy(memberGroupStrategy);
      return this;
    }

    /**
     * Sets the storage level.
     *
     * @param storageLevel the storage level
     * @return the Raft partition group builder
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      config.getStorageConfig().setLevel(storageLevel);
      return this;
    }

    /**
     * Sets the path to the data directory.
     *
     * @param dataDir the path to the replica's data directory
     * @return the replica builder
     */
    public Builder withDataDirectory(File dataDir) {
      config.getStorageConfig().setDirectory(new File("user.dir").toURI().relativize(dataDir.toURI()).getPath());
      return this;
    }

    /**
     * Sets the segment size.
     *
     * @param segmentSize the segment size
     * @return the partition group builder
     */
    public Builder withSegmentSize(MemorySize segmentSize) {
      config.getStorageConfig().setSegmentSize(segmentSize);
      return this;
    }

    /**
     * Sets the segment size.
     *
     * @param segmentSizeBytes the segment size in bytes
     * @return the partition group builder
     */
    public Builder withSegmentSize(long segmentSizeBytes) {
      return withSegmentSize(new MemorySize(segmentSizeBytes));
    }

    /**
     * Sets the maximum Raft log entry size.
     *
     * @param maxEntrySize the maximum Raft log entry size
     * @return the partition group builder
     */
    public Builder withMaxEntrySize(MemorySize maxEntrySize) {
      config.getStorageConfig().setMaxEntrySize(maxEntrySize);
      return this;
    }

    /**
     * Sets the maximum Raft log entry size.
     *
     * @param maxEntrySize the maximum Raft log entry size
     * @return the partition group builder
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      return withMaxEntrySize(new MemorySize(maxEntrySize));
    }

    /**
     * Enables flush on commit.
     *
     * @return the partition group builder
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush logs to disk on commit.
     *
     * @param flushOnCommit whether to flush logs to disk on commit
     * @return the partition group builder
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      config.getStorageConfig().setFlushOnCommit(flushOnCommit);
      return this;
    }

    /**
     * Sets the maximum size of the log.
     *
     * @param maxSize the maximum size of the log
     * @return the partition group builder
     */
    public Builder withMaxSize(long maxSize) {
      config.getCompactionConfig().setSize(MemorySize.from(maxSize));
      return this;
    }

    /**
     * Sets the maximum age of the log.
     *
     * @param maxAge the maximum age of the log
     * @return the partition group builder
     */
    public Builder withMaxAge(Duration maxAge) {
      config.getCompactionConfig().setAge(maxAge);
      return this;
    }

    @Override
    public LogPartitionGroup build() {
      return new LogPartitionGroup(config);
    }
  }
}
