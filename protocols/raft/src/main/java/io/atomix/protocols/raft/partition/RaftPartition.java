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

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.protocols.raft.partition.impl.RaftClientCommunicator;
import io.atomix.protocols.raft.partition.impl.RaftNamespaces;
import io.atomix.protocols.raft.partition.impl.RaftPartitionClient;
import io.atomix.protocols.raft.partition.impl.RaftPartitionServer;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Abstract partition.
 */
public class RaftPartition implements Partition {
  private final PartitionId partitionId;
  private final RaftPartitionGroupConfig config;
  private final File dataDirectory;
  private final ThreadContextFactory threadContextFactory;
  private PartitionMetadata partition;
  private RaftPartitionClient client;
  private RaftPartitionServer server;

  public RaftPartition(
      PartitionId partitionId,
      RaftPartitionGroupConfig config,
      File dataDirectory,
      ThreadContextFactory threadContextFactory) {
    this.partitionId = partitionId;
    this.config = config;
    this.dataDirectory = dataDirectory;
    this.threadContextFactory = threadContextFactory;
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
    return String.format("%s-partition-%d", partitionId.group(), partitionId.id());
  }

  @Override
  public long term() {
    return client != null ? client.term() : 0;
  }

  @Override
  public MemberId primary() {
    return client != null ? client.leader() : null;
  }

  @Override
  public Collection<MemberId> backups() {
    MemberId leader = primary();
    if (leader == null) {
      return members();
    }
    return members().stream()
        .filter(m -> !m.equals(leader))
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<MemberId> members() {
    return partition != null ? partition.members() : Collections.emptyList();
  }

  /**
   * Returns the partition data directory.
   *
   * @return the partition data directory
   */
  public File dataDirectory() {
    return dataDirectory;
  }

  /**
   * Takes a snapshot of the partition.
   *
   * @return a future to be completed once the snapshot is complete
   */
  public CompletableFuture<Void> snapshot() {
    RaftPartitionServer server = this.server;
    if (server != null) {
      return server.snapshot();
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public RaftPartitionClient getClient() {
    return client;
  }

  /**
   * Opens the partition.
   */
  CompletableFuture<Partition> open(PartitionMetadata metadata, PartitionManagementService managementService) {
    this.partition = metadata;
    this.client = createClient(managementService);
    if (partition.members().contains(managementService.getMembershipService().getLocalMember().id())) {
      server = createServer(managementService);
      return server.start()
          .thenCompose(v -> client.start())
          .thenApply(v -> null);
    }
    return client.start()
        .thenApply(v -> this);
  }

  /**
   * Updates the partition with the given metadata.
   */
  CompletableFuture<Void> update(PartitionMetadata metadata, PartitionManagementService managementService) {
    if (server == null && metadata.members().contains(managementService.getMembershipService().getLocalMember().id())) {
      server = createServer(managementService);
      return server.join(metadata.members());
    } else if (server != null && !metadata.members().contains(managementService.getMembershipService().getLocalMember().id())) {
      return server.leave().thenRun(() -> server = null);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Closes the partition.
   */
  CompletableFuture<Void> close() {
    return closeClient()
        .exceptionally(v -> null)
        .thenCompose(v -> closeServer())
        .exceptionally(v -> null);
  }

  private CompletableFuture<Void> closeClient() {
    if (client != null) {
      return client.stop();
    }
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<Void> closeServer() {
    if (server != null) {
      return server.stop();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Creates a Raft server.
   */
  protected RaftPartitionServer createServer(PartitionManagementService managementService) {
    return new RaftPartitionServer(
        this,
        config,
        managementService.getMembershipService().getLocalMember().id(),
        managementService.getMembershipService(),
        managementService.getMessagingService(),
        managementService.getPrimitiveTypes(),
        threadContextFactory);
  }

  /**
   * Creates a Raft client.
   */
  private RaftPartitionClient createClient(PartitionManagementService managementService) {
    return new RaftPartitionClient(
        this,
        managementService.getMembershipService().getLocalMember().id(),
        new RaftClientCommunicator(
            name(),
            Serializer.using(RaftNamespaces.RAFT_PROTOCOL),
            managementService.getMessagingService()),
        threadContextFactory);
  }

  /**
   * Deletes the partition.
   *
   * @return future to be completed once the partition has been deleted
   */
  public CompletableFuture<Void> delete() {
    return server.stop().thenCompose(v -> client.stop()).thenRun(() -> {
      if (server != null) {
        server.delete();
      }
    });
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionId", id())
        .toString();
  }
}
