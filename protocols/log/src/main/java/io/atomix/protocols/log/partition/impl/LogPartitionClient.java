// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.partition.impl;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionClient;
import io.atomix.protocols.log.DistributedLogSessionClient;
import io.atomix.protocols.log.partition.LogPartition;
import io.atomix.protocols.log.serializer.impl.LogNamespaces;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Primary-backup partition client.
 */
public class LogPartitionClient implements PartitionClient, Managed<LogPartitionClient> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final LogPartition partition;
  private final PartitionManagementService managementService;
  private final ThreadContextFactory threadFactory;
  private volatile DistributedLogSessionClient client;

  public LogPartitionClient(
      LogPartition partition,
      PartitionManagementService managementService,
      ThreadContextFactory threadFactory) {
    this.partition = partition;
    this.managementService = managementService;
    this.threadFactory = threadFactory;
  }

  /**
   * Returns a new log session builder.
   *
   * @return a new log session builder
   */
  public LogSession.Builder logSessionBuilder() {
    return client.sessionBuilder();
  }

  @Override
  public SessionClient.Builder sessionBuilder(String primitiveName, PrimitiveType primitiveType, ServiceConfig serviceConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<LogPartitionClient> start() {
    synchronized (LogPartitionClient.this) {
      client = newClient();
      log.debug("Successfully started client for {}", partition.id());
    }
    return CompletableFuture.completedFuture(this);
  }

  private DistributedLogSessionClient newClient() {
    return DistributedLogSessionClient.builder()
        .withClientName(partition.name())
        .withPartitionId(partition.id())
        .withMembershipService(managementService.getMembershipService())
        .withProtocol(new LogClientCommunicator(
            partition.name(),
            Serializer.using(LogNamespaces.PROTOCOL),
            managementService.getMessagingService()))
        .withSessionIdProvider(() -> managementService.getSessionIdService().nextSessionId())
        .withPrimaryElection(managementService.getElectionService().getElectionFor(partition.id()))
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
