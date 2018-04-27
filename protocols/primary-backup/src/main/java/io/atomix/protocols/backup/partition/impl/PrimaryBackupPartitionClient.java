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

import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.protocols.backup.PrimaryBackupClient;
import io.atomix.protocols.backup.partition.PrimaryBackupPartition;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupNamespaces;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Primary-backup partition client.
 */
public class PrimaryBackupPartitionClient implements Managed<PrimaryBackupPartitionClient> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final PrimaryBackupPartition partition;
  private final PartitionManagementService managementService;
  private final ThreadContextFactory threadFactory;
  private PrimaryBackupClient client;

  public PrimaryBackupPartitionClient(
      PrimaryBackupPartition partition,
      PartitionManagementService managementService,
      ThreadContextFactory threadFactory) {
    this.partition = partition;
    this.managementService = managementService;
    this.threadFactory = threadFactory;
  }

  /**
   * Returns the proxy client.
   *
   * @return the proxy client
   */
  public PrimaryBackupClient getProxyClient() {
    return client;
  }

  @Override
  public CompletableFuture<PrimaryBackupPartitionClient> start() {
    synchronized (PrimaryBackupPartitionClient.this) {
      client = newClient();
      log.info("Successfully started client for {}", partition.id());
    }
    return CompletableFuture.completedFuture(this);
  }

  private PrimaryBackupClient newClient() {
    return PrimaryBackupClient.builder()
        .withClientName(partition.name())
        .withPartitionId(partition.id())
        .withMembershipService(managementService.getMembershipService())
        .withProtocol(new PrimaryBackupClientCommunicator(
            partition.name(),
            Serializer.using(PrimaryBackupNamespaces.PROTOCOL),
            managementService.getMessagingService()))
        .withPrimaryElection(managementService.getElectionService().getElectionFor(partition.id()))
        .withSessionIdProvider(managementService.getSessionIdService())
        .withThreadContextFactory(threadFactory)
        .build();
  }

  @Override
  public boolean isRunning() {
    return client != null;
  }

  @Override
  public CompletableFuture<Void> stop() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }
}
