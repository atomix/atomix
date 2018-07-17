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

import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.protocols.backup.PrimaryBackupServer;
import io.atomix.protocols.backup.partition.PrimaryBackupPartition;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupNamespaces;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Primary-backup partition server.
 */
public class PrimaryBackupPartitionServer implements Managed<PrimaryBackupPartitionServer> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final PrimaryBackupPartition partition;
  private final PartitionManagementService managementService;
  private final MemberGroupProvider memberGroupProvider;
  private final ThreadContextFactory threadFactory;
  private PrimaryBackupServer server;
  private final AtomicBoolean started = new AtomicBoolean();

  public PrimaryBackupPartitionServer(
      PrimaryBackupPartition partition,
      PartitionManagementService managementService,
      MemberGroupProvider memberGroupProvider,
      ThreadContextFactory threadFactory) {
    this.partition = partition;
    this.managementService = managementService;
    this.memberGroupProvider = memberGroupProvider;
    this.threadFactory = threadFactory;
  }

  @Override
  public CompletableFuture<PrimaryBackupPartitionServer> start() {
    synchronized (this) {
      server = buildServer();
    }
    return server.start().thenApply(s -> {
      log.debug("Successfully started server for {}", partition.id());
      started.set(true);
      return this;
    });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  private PrimaryBackupServer buildServer() {
    return PrimaryBackupServer.builder()
        .withServerName(partition.name())
        .withMembershipService(managementService.getMembershipService())
        .withMemberGroupProvider(memberGroupProvider)
        .withProtocol(new PrimaryBackupServerCommunicator(
            partition.name(),
            Serializer.using(PrimaryBackupNamespaces.PROTOCOL),
            managementService.getMessagingService()))
        .withPrimaryElection(managementService.getElectionService().getElectionFor(partition.id()))
        .withPrimitiveTypes(managementService.getPrimitiveTypes())
        .withThreadContextFactory(threadFactory)
        .build();
  }

  @Override
  public CompletableFuture<Void> stop() {
    PrimaryBackupServer server = this.server;
    if (server != null) {
      return server.stop().exceptionally(throwable -> {
        log.error("Failed stopping server for {}", partition.id(), throwable);
        return null;
      }).thenRun(() -> started.set(false));
    }
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
