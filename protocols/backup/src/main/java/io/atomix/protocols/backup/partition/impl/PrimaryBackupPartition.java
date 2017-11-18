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
package io.atomix.protocols.backup.partition.impl;

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartition;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.protocols.backup.ReplicaInfoProvider;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Primary-backup partition.
 */
public class PrimaryBackupPartition implements ManagedPartition {
  private final PartitionId partitionId;
  private final PrimaryBackupPartitionServer server;
  private final PrimaryBackupPartitionClient client;
  private final AtomicBoolean open = new AtomicBoolean();

  public PrimaryBackupPartition(
      PartitionId partitionId,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ReplicaInfoProvider replicaProvider,
      PrimitiveTypeRegistry primitiveTypes) {
    this.partitionId = partitionId;
    this.server = new PrimaryBackupPartitionServer(this, clusterService, communicationService, replicaProvider, primitiveTypes);
    this.client = new PrimaryBackupPartitionClient(this, clusterService, communicationService, replicaProvider);
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
    return String.format("partition-%d", partitionId.id());
  }

  @Override
  public PrimitiveClient getPrimitiveClient() {
    return client;
  }

  @Override
  public CompletableFuture<Partition> open() {
    return server.open().thenCompose(v -> client.open()).thenApply(v -> {
      open.set(true);
      return this;
    });
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    client.close().whenComplete((result, error) -> {
      server.close().whenComplete((serverResult, serverError) -> {
        open.set(false);
        future.complete(null);
      });
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
