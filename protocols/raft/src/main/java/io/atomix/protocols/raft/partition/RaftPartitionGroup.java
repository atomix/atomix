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
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.impl.DefaultRaftClient;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.memory.MemorySize;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft partition group.
 */
public class RaftPartitionGroup implements ManagedPartitionGroup {
  public static final Type TYPE = new Type();

  /**
   * Returns a new Raft partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(new RaftPartitionGroupConfig().setName(name));
  }

  /**
   * Raft partition group type.
   */
  public static class Type implements PartitionGroup.Type<RaftPartitionGroupConfig> {
    private static final String NAME = "raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Namespace namespace() {
      return Namespace.builder()
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 100)
          .register(RaftPartitionGroupConfig.class)
          .register(RaftStorageConfig.class)
          .register(RaftCompactionConfig.class)
          .register(StorageLevel.class)
          .build();
    }

    @Override
    public RaftPartitionGroupConfig newConfig() {
      return new RaftPartitionGroupConfig();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(RaftPartitionGroupConfig config) {
      return new RaftPartitionGroup(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftPartitionGroup.class);

  private static Collection<RaftPartition> buildPartitions(
      RaftPartitionGroupConfig config,
      ThreadContextFactory threadContextFactory) {
    File partitionsDir = new File(config.getStorageConfig().getDirectory(config.getName()), "partitions");
    List<RaftPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new RaftPartition(
          PartitionId.from(config.getName(), i + 1),
          config,
          new File(partitionsDir, String.valueOf(i + 1)),
          threadContextFactory));
    }
    return partitions;
  }

  private static final Duration SNAPSHOT_TIMEOUT = Duration.ofSeconds(15);

  private final String name;
  private final RaftPartitionGroupConfig config;
  private final int partitionSize;
  private final ThreadContextFactory threadContextFactory;
  private final Map<PartitionId, RaftPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private Collection<PartitionMetadata> metadata;
  private ClusterCommunicationService communicationService;
  private final String snapshotSubject;

  public RaftPartitionGroup(RaftPartitionGroupConfig config) {
    Logger log = ContextualLoggerFactory.getLogger(DefaultRaftClient.class, LoggerContext.builder(RaftClient.class)
        .addValue(config.getName())
        .build());
    this.name = config.getName();
    this.config = config;
    this.partitionSize = config.getPartitionSize();

    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "raft-partition-group-" + name + "-%d", threadPoolSize, log);
    this.snapshotSubject = "raft-partition-group-" + name + "-snapshot";

    buildPartitions(config, threadContextFactory).forEach(p -> {
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
  public PartitionGroup.Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProtocol.Type protocol() {
    return MultiRaftProtocol.TYPE;
  }

  @Override
  public PartitionGroupConfig config() {
    return config;
  }

  @Override
  public ProxyProtocol newProtocol() {
    return MultiRaftProtocol.builder(name)
        .withRecoveryStrategy(Recovery.RECOVER)
        .withMaxRetries(5)
        .build();
  }

  @Override
  public RaftPartition getPartition(PartitionId partitionId) {
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

  /**
   * Takes snapshots of all Raft partitions.
   *
   * @return a future to be completed once snapshots have been taken
   */
  public CompletableFuture<Void> snapshot() {
    return Futures.allOf(config.getMembers().stream()
        .map(MemberId::from)
        .map(id -> communicationService.send(snapshotSubject, null, id, SNAPSHOT_TIMEOUT))
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Handles a snapshot request from a peer.
   *
   * @return a future to be completed once the snapshot is complete
   */
  private CompletableFuture<Void> handleSnapshot() {
    return Futures.allOf(partitions.values().stream()
        .map(partition -> partition.snapshot())
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    this.metadata = buildPartitions();
    this.communicationService = managementService.getMessagingService();
    communicationService.<Void, Void>subscribe(snapshotSubject, m -> handleSnapshot());
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

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    return join(managementService);
  }

  private Collection<PartitionMetadata> buildPartitions() {
    List<MemberId> sorted = new ArrayList<>(config.getMembers().stream()
        .map(MemberId::from)
        .collect(Collectors.toSet()));
    Collections.sort(sorted);

    int partitionSize = this.partitionSize;
    if (partitionSize == 0) {
      partitionSize = sorted.size();
    }

    int length = sorted.size();
    int count = Math.min(partitionSize, length);

    Set<PartitionMetadata> metadata = Sets.newHashSet();
    for (int i = 0; i < partitions.size(); i++) {
      PartitionId partitionId = sortedPartitionIds.get(i);
      Set<MemberId> set = new HashSet<>(count);
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
      threadContextFactory.close();
      communicationService.unsubscribe(snapshotSubject);
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
  public static class Builder extends PartitionGroup.Builder<RaftPartitionGroupConfig> {
    protected Builder(RaftPartitionGroupConfig config) {
      super(config);
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(String... members) {
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(MemberId... members) {
      return withMembers(Stream.of(members).map(nodeId -> nodeId.id()).collect(Collectors.toList()));
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(Member... members) {
      return withMembers(Stream.of(members).map(node -> node.id().id()).collect(Collectors.toList()));
    }

    /**
     * Sets the Raft partition group members.
     *
     * @param members the Raft partition group members
     * @return the Raft partition group builder
     * @throws NullPointerException if the members are null
     */
    public Builder withMembers(Collection<String> members) {
      config.setMembers(Sets.newHashSet(checkNotNull(members, "members cannot be null")));
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the Raft partition group builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    /**
     * Sets the partition size.
     *
     * @param partitionSize the partition size
     * @return the Raft partition group builder
     * @throws IllegalArgumentException if the partition size is not positive
     */
    public Builder withPartitionSize(int partitionSize) {
      config.setPartitionSize(partitionSize);
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
     * @return the Raft partition group builder
     */
    public Builder withSegmentSize(MemorySize segmentSize) {
      config.getStorageConfig().setSegmentSize(segmentSize);
      return this;
    }

    /**
     * Sets the segment size.
     *
     * @param segmentSizeBytes the segment size in bytes
     * @return the Raft partition group builder
     */
    public Builder withSegmentSize(long segmentSizeBytes) {
      return withSegmentSize(new MemorySize(segmentSizeBytes));
    }

    /**
     * Sets the maximum Raft log entry size.
     *
     * @param maxEntrySize the maximum Raft log entry size
     * @return the Raft partition group builder
     */
    public Builder withMaxEntrySize(MemorySize maxEntrySize) {
      config.getStorageConfig().setMaxEntrySize(maxEntrySize);
      return this;
    }

    /**
     * Sets the maximum Raft log entry size.
     *
     * @param maxEntrySize the maximum Raft log entry size
     * @return the Raft partition group builder
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      return withMaxEntrySize(new MemorySize(maxEntrySize));
    }

    /**
     * Enables flush on commit.
     *
     * @return the Raft partition group builder
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush logs to disk on commit.
     *
     * @param flushOnCommit whether to flush logs to disk on commit
     * @return the Raft partition group  builder
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      config.getStorageConfig().setFlushOnCommit(flushOnCommit);
      return this;
    }

    @Override
    public RaftPartitionGroup build() {
      return new RaftPartitionGroup(config);
    }
  }
}
