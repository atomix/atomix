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
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.proxy.PrimitiveProxy.Builder;
import io.atomix.protocols.backup.PrimaryBackupClient;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.utils.Managed;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Primary-backup partition client.
 */
public class PrimaryBackupPartitionClient implements PrimitiveClient, Managed<PrimaryBackupPartitionClient> {
  private final PrimaryBackupPartition partition;
  private final ClusterService clusterService;
  private final ClusterCommunicationService clusterCommunicator;
  private final ReplicaInfoProvider replicaProvider;
  private PrimaryBackupClient client;

  public PrimaryBackupPartitionClient(
      PrimaryBackupPartition partition,
      ClusterService clusterService,
      ClusterCommunicationService clusterCommunicator,
      ReplicaInfoProvider replicaProvider) {
    this.partition = partition;
    this.clusterService = clusterService;
    this.clusterCommunicator = clusterCommunicator;
    this.replicaProvider = replicaProvider;
  }

  @Override
  public Builder proxyBuilder(String primitiveName, PrimitiveType primitiveType) {
    return client.proxyBuilder(primitiveName, primitiveType);
  }

  @Override
  public CompletableFuture<Set<String>> getPrimitives(PrimitiveType primitiveType) {
    return client.getPrimitives(primitiveType);
  }

  @Override
  public CompletableFuture<PrimaryBackupPartitionClient> open() {
    synchronized (PrimaryBackupPartitionClient.this) {
      client = newClient();
    }
    return CompletableFuture.completedFuture(this);
  }

  private PrimaryBackupClient newClient() {
    return PrimaryBackupClient.builder()
        .withClusterService(clusterService)
        .withCommunicationService(clusterCommunicator)
        .withReplicaProvider(replicaProvider)
        .build();
  }

  @Override
  public boolean isOpen() {
    return client != null;
  }

  @Override
  public CompletableFuture<Void> close() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return client == null;
  }
}
