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
package io.atomix.protocols.raft.partition.impl;

import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartition;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.utils.serializer.Serializer;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Abstract partition.
 */
public class RaftPartition implements ManagedPartition {
  protected final AtomicBoolean isOpened = new AtomicBoolean(false);
  protected final ClusterCommunicationService clusterCommunicator;
  protected final PrimitiveTypeRegistry primitiveTypes;
  protected final PartitionMetadata partition;
  protected final NodeId localNodeId;
  private final File dataDir;
  private final RaftPartitionClient client;
  private final RaftPartitionServer server;

  public RaftPartition(
      NodeId nodeId,
      PartitionMetadata partition,
      ClusterCommunicationService clusterCommunicator,
      PrimitiveTypeRegistry primitiveTypes,
      File dataDir) {
    this.localNodeId = nodeId;
    this.partition = partition;
    this.clusterCommunicator = clusterCommunicator;
    this.primitiveTypes = primitiveTypes;
    this.dataDir = dataDir;
    this.client = createClient();
    this.server = createServer();
  }

  @Override
  public PartitionId id() {
    return partition.id();
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
   * Returns the partition data directory.
   *
   * @return the partition data directory
   */
  File getDataDir() {
    return dataDir;
  }

  @Override
  public PrimitiveClient getPrimitiveClient() {
    return client;
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
        localNodeId,
        clusterCommunicator,
        primitiveTypes);
  }

  /**
   * Creates a Raft client.
   */
  private RaftPartitionClient createClient() {
    return new RaftPartitionClient(
        this,
        localNodeId,
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
