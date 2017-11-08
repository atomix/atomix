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
import io.atomix.cluster.messaging.ClusterCommunicator;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionMetadata;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

/**
 * Client partition.
 */
public class ClientPartition extends RaftPartition {
  private final RaftPartitionClient client;

  public ClientPartition(
      NodeId nodeId,
      PartitionMetadata partition,
      ClusterCommunicator clusterCommunicator) {
    super(nodeId, partition, clusterCommunicator);
    this.client = createClient();
  }

  @Override
  public DistributedPrimitiveCreator getPrimitiveCreator() {
    return client;
  }

  @Override
  public CompletableFuture<Partition> open() {
    return client.open()
        .thenAccept(v -> isOpened.set(true))
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
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
}
