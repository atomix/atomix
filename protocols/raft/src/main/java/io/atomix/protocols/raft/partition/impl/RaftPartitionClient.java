// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.partition.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.session.RaftSessionClient;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * StoragePartition client.
 */
public class RaftPartitionClient implements PartitionClient, Managed<RaftPartitionClient> {

  private final Logger log = getLogger(getClass());

  private final RaftPartition partition;
  private final MemberId localMemberId;
  private final RaftClientProtocol protocol;
  private final ThreadContextFactory threadContextFactory;
  private RaftClient client;

  public RaftPartitionClient(
      RaftPartition partition,
      MemberId localMemberId,
      RaftClientProtocol protocol,
      ThreadContextFactory threadContextFactory) {
    this.partition = partition;
    this.localMemberId = localMemberId;
    this.protocol = protocol;
    this.threadContextFactory = threadContextFactory;
  }

  /**
   * Returns the partition term.
   *
   * @return the partition term
   */
  public long term() {
    return client != null ? client.term() : 0;
  }

  /**
   * Returns the partition leader.
   *
   * @return the partition leader
   */
  public MemberId leader() {
    return client != null ? client.leader() : null;
  }

  @Override
  public RaftSessionClient.Builder sessionBuilder(String primitiveName, PrimitiveType primitiveType, ServiceConfig serviceConfig) {
    return client.sessionBuilder(primitiveName, primitiveType, serviceConfig);
  }

  @Override
  public CompletableFuture<RaftPartitionClient> start() {
    synchronized (RaftPartitionClient.this) {
      client = newRaftClient(protocol);
    }
    return client.connect(partition.members()).whenComplete((r, e) -> {
      if (e == null) {
        log.debug("Successfully started client for partition {}", partition.id());
      } else {
        log.warn("Failed to start client for partition {}", partition.id(), e);
      }
    }).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return client != null;
  }

  private RaftClient newRaftClient(RaftClientProtocol protocol) {
    return RaftClient.builder()
        .withClientId(partition.name())
        .withPartitionId(partition.id())
        .withMemberId(localMemberId)
        .withProtocol(protocol)
        .withThreadContextFactory(threadContextFactory)
        .build();
  }
}
