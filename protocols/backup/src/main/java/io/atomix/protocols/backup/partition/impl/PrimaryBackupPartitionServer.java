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
import io.atomix.cluster.Node;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.protocols.backup.PrimaryBackupServer;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.utils.Managed;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Primary-backup partition server.
 */
public class PrimaryBackupPartitionServer implements Managed<PrimaryBackupPartitionServer> {
  private final PrimaryBackupPartition partition;
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final ReplicaInfoProvider replicaProvider;
  private PrimaryBackupServer server;
  private final AtomicBoolean open = new AtomicBoolean();

  public PrimaryBackupPartitionServer(
      PrimaryBackupPartition partition,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ReplicaInfoProvider replicaProvider) {
    this.partition = partition;
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.replicaProvider = replicaProvider;
  }

  @Override
  public CompletableFuture<PrimaryBackupPartitionServer> open() {
    if (clusterService.getLocalNode().type() == Node.Type.CORE) {
      synchronized (this) {
        server = buildServer();
      }
      return server.open().thenApply(v -> {
        open.set(true);
        return this;
      });
    }
    open.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  private PrimaryBackupServer buildServer() {
    PrimaryBackupServer.Builder builder = PrimaryBackupServer.builder()
        .withServerName(partition.name())
        .withClusterService(clusterService)
        .withCommunicationService(communicationService)
        .withReplicaProvider(replicaProvider);
    PrimaryBackupPartition.SERVICES.forEach(builder::addService);
    return builder.build();
  }

  @Override
  public CompletableFuture<Void> close() {
    PrimaryBackupServer server = this.server;
    if (server != null) {
      return server.close().thenRun(() -> open.set(false));
    }
    open.set(false);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
