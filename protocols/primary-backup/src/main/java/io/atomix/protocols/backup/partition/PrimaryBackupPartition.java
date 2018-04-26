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
package io.atomix.protocols.backup.partition;

import io.atomix.cluster.NodeId;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.partition.Member;
import io.atomix.primitive.partition.MemberFilter;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.backup.partition.impl.PrimaryBackupPartitionClient;
import io.atomix.protocols.backup.partition.impl.PrimaryBackupPartitionServer;
import io.atomix.utils.concurrent.ThreadContextFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary-backup partition.
 */
public class PrimaryBackupPartition implements Partition<MultiPrimaryProtocol> {
  private final PartitionId partitionId;
  private final MemberFilter memberFilter;
  private final MemberGroupProvider memberGroupProvider;
  private PrimaryElection election;
  private PrimaryBackupPartitionServer server;
  private PrimaryBackupPartitionClient client;

  public PrimaryBackupPartition(
      PartitionId partitionId,
      MemberFilter memberFilter,
      MemberGroupProvider memberGroupProvider) {
    this.partitionId = partitionId;
    this.memberFilter = memberFilter;
    this.memberGroupProvider = memberGroupProvider;
  }

  @Override
  public PartitionId id() {
    return partitionId;
  }

  @Override
  public long term() {
    return election.getTerm().join().term();
  }

  @Override
  public NodeId primary() {
    return election.getTerm()
        .join()
        .primary()
        .nodeId();
  }

  @Override
  public Collection<NodeId> backups() {
    return election.getTerm()
        .join()
        .candidates()
        .stream()
        .map(Member::nodeId)
        .collect(Collectors.toList());
  }

  /**
   * Returns the partition name.
   *
   * @return the partition name
   */
  public String name() {
    return String.format("%s-partition-%d", partitionId.group(), partitionId.id());
  }

  @Override
  public PrimitiveClient<MultiPrimaryProtocol> getPrimitiveClient() {
    return client;
  }

  /**
   * Opens the primary-backup partition.
   */
  CompletableFuture<Partition> open(
      PartitionManagementService managementService,
      ThreadContextFactory threadFactory) {
    election = managementService.getElectionService().getElectionFor(partitionId);
    server = new PrimaryBackupPartitionServer(
        this,
        managementService,
        memberFilter,
        memberGroupProvider,
        threadFactory);
    client = new PrimaryBackupPartitionClient(this, managementService, threadFactory);
    return server.start().thenCompose(v -> client.start()).thenApply(v -> this);
  }

  /**
   * Closes the primary-backup partition.
   */
  public CompletableFuture<Void> close() {
    if (client == null) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    client.stop().whenComplete((clientResult, clientError) -> {
      server.stop().whenComplete((serverResult, serverError) -> {
        future.complete(null);
      });
    });
    return future;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", partitionId)
        .toString();
  }
}
