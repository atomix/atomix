/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.partition.impl;

import io.atomix.cluster.MemberId;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.utils.Managed;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * StoragePartition client.
 */
public class RaftPartitionClient implements Managed<RaftPartitionClient> {

  private final Logger log = getLogger(getClass());

  private final RaftPartition partition;
  private final MemberId localMemberId;
  private final RaftClientProtocol protocol;
  private RaftClient client;

  public RaftPartitionClient(RaftPartition partition, MemberId localMemberId, RaftClientProtocol protocol) {
    this.partition = partition;
    this.localMemberId = localMemberId;
    this.protocol = protocol;
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

  /**
   * Returns the proxy client.
   *
   * @return the proxy client
   */
  public RaftClient getProxyClient() {
    return client;
  }

  @Override
  public CompletableFuture<RaftPartitionClient> start() {
    synchronized (RaftPartitionClient.this) {
      client = newRaftClient(protocol);
    }
    return client.connect(partition.members()).whenComplete((r, e) -> {
      if (e == null) {
        log.info("Successfully started client for partition {}", partition.id());
      } else {
        log.info("Failed to start client for partition {}", partition.id(), e);
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
        .build();
  }
}
