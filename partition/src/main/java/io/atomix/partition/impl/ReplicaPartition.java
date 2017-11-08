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
package io.atomix.partition.impl;

import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionMetadata;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.serializer.Serializer;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Replica partition.
 */
public class ReplicaPartition extends RaftPartition {
  private final File dataFolder;
  private final RaftPartitionClient client;
  private final RaftPartitionServer server;

  public ReplicaPartition(
      NodeId nodeId,
      PartitionMetadata partition,
      ClusterCommunicationService clusterCommunicator,
      File dataFolder) {
    super(nodeId, partition, clusterCommunicator);
    this.dataFolder = dataFolder;
    this.client = createClient();
    this.server = createServer();
  }

  @Override
  public DistributedPrimitiveCreator getPrimitiveCreator() {
    return client;
  }

  /**
   * Returns the partition data folder.
   *
   * @return the partition data folder
   */
  public File getDataFolder() {
    return dataFolder;
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
  public CompletableFuture<Void> close() {
    // We do not explicitly close the server and instead let the cluster
    // deal with this as an unclean exit.
    return client.close();
  }

  /**
   * Creates a Raft server.
   */
  protected RaftPartitionServer createServer() {
    return new RaftPartitionServer(
        this,
        MemberId.from(localNodeId.id()),
        clusterCommunicator);
  }

  /**
   * Creates a Raft client.
   */
  private RaftPartitionClient createClient() {
    return new RaftPartitionClient(
        this,
        MemberId.from(localNodeId.id()),
        new RaftClientCommunicator(
            getName(),
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
}
