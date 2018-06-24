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

import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.backup.partition.impl.PrimaryBackupPartitionClient;
import io.atomix.protocols.backup.partition.impl.PrimaryBackupPartitionServer;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary-backup partition.
 */
public class PrimaryBackupPartition implements Partition {
  private final PartitionId partitionId;
  private final MemberGroupProvider memberGroupProvider;
  private PrimaryElection election;
  private PrimaryBackupPartitionServer server;
  private PrimaryBackupPartitionClient client;

  public PrimaryBackupPartition(
      PartitionId partitionId,
      MemberGroupProvider memberGroupProvider) {
    this.partitionId = partitionId;
    this.memberGroupProvider = memberGroupProvider;
  }

  @Override
  public PartitionId id() {
    return partitionId;
  }

  @Override
  public long term() {
    return Futures.get(election.getTerm()).term();
  }

  @Override
  public Collection<MemberId> members() {
    return Futures.get(election.getTerm())
        .candidates()
        .stream()
        .map(GroupMember::memberId)
        .collect(Collectors.toList());
  }

  @Override
  public MemberId primary() {
    return Futures.get(election.getTerm())
        .primary()
        .memberId();
  }

  @Override
  public Collection<MemberId> backups() {
    return Futures.get(election.getTerm())
        .candidates()
        .stream()
        .map(GroupMember::memberId)
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
  public PrimaryBackupPartitionClient getClient() {
    return client;
  }

  /**
   * Joins the primary-backup partition.
   */
  CompletableFuture<Partition> join(
      PartitionManagementService managementService,
      ThreadContextFactory threadFactory) {
    election = managementService.getElectionService().getElectionFor(partitionId);
    server = new PrimaryBackupPartitionServer(
        this,
        managementService,
        memberGroupProvider,
        threadFactory);
    client = new PrimaryBackupPartitionClient(this, managementService, threadFactory);
    return server.start().thenCompose(v -> client.start()).thenApply(v -> this);
  }

  /**
   * Connects to the primary-backup partition.
   */
  CompletableFuture<Partition> connect(
      PartitionManagementService managementService,
      ThreadContextFactory threadFactory) {
    election = managementService.getElectionService().getElectionFor(partitionId);
    client = new PrimaryBackupPartitionClient(this, managementService, threadFactory);
    return client.start().thenApply(v -> this);
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
      if (server != null) {
        server.stop().whenComplete((serverResult, serverError) -> {
          future.complete(null);
        });
      } else {
        future.complete(null);
      }
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
