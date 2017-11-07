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
package io.atomix.partition.impl;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.partition.Partition;
import io.atomix.partition.PartitionId;
import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitive.Type;
import io.atomix.primitives.Ordering;
import io.atomix.primitives.counter.impl.RaftCounterService;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorService;
import io.atomix.primitives.map.impl.RaftAtomicCounterMapService;
import io.atomix.primitives.map.impl.RaftConsistentMapService;
import io.atomix.primitives.map.impl.RaftConsistentTreeMapService;
import io.atomix.primitives.multimap.impl.RaftConsistentSetMultimapService;
import io.atomix.primitives.queue.impl.RaftWorkQueueService;
import io.atomix.primitives.tree.impl.RaftDocumentTreeService;
import io.atomix.primitives.value.impl.RaftAtomicValueService;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.protocol.messaging.RaftClientCommunicator;
import io.atomix.protocols.raft.service.RaftService;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.impl.StorageNamespaces;
import io.atomix.utils.Managed;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Storage partition.
 */
public abstract class AtomixPartition implements Managed<AtomixPartition> {

  static final String PARTITIONS_DIR =
      System.getProperty("karaf.data") + "/db/partitions/";

  protected final AtomicBoolean isOpened = new AtomicBoolean(false);
  protected final ClusterCommunicationService clusterCommunicator;
  protected Partition partition;
  protected NodeId localNodeId;
  protected AtomixPartitionServer server;
  protected AtomixPartitionClient client;

  public static final Map<String, Supplier<RaftService>> RAFT_SERVICES =
      ImmutableMap.<String, Supplier<RaftService>>builder()
          .put(DistributedPrimitive.Type.CONSISTENT_MAP.name(), RaftConsistentMapService::new)
          .put(DistributedPrimitive.Type.CONSISTENT_TREEMAP.name(), RaftConsistentTreeMapService::new)
          .put(DistributedPrimitive.Type.CONSISTENT_MULTIMAP.name(), RaftConsistentSetMultimapService::new)
          .put(DistributedPrimitive.Type.COUNTER_MAP.name(), RaftAtomicCounterMapService::new)
          .put(DistributedPrimitive.Type.COUNTER.name(), RaftCounterService::new)
          .put(DistributedPrimitive.Type.LEADER_ELECTOR.name(), RaftLeaderElectorService::new)
          .put(DistributedPrimitive.Type.WORK_QUEUE.name(), RaftWorkQueueService::new)
          .put(Type.VALUE.name(), RaftAtomicValueService::new)
          .put(DistributedPrimitive.Type.DOCUMENT_TREE.name(),
              () -> new RaftDocumentTreeService(Ordering.NATURAL))
          .put(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), Ordering.NATURAL),
              () -> new RaftDocumentTreeService(Ordering.NATURAL))
          .put(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), Ordering.INSERTION),
              () -> new RaftDocumentTreeService(Ordering.INSERTION))
          .build();

  public AtomixPartition(
      Partition partition,
      ClusterCommunicationService clusterCommunicator,
      ClusterService clusterService) {
    this.partition = partition;
    this.clusterCommunicator = clusterCommunicator;
    this.localNodeId = clusterService.getLocalNode().id();
  }

  /**
   * Returns the partition client instance.
   *
   * @return client
   */
  public AtomixPartitionClient client() {
    return client;
  }

  @Override
  public CompletableFuture<AtomixPartition> open() {
    if (partition.members().contains(localNodeId)) {
      return openServer()
          .thenCompose(v -> openClient())
          .thenAccept(v -> isOpened.set(true))
          .thenApply(v -> null);
    }
    return openClient()
        .thenAccept(v -> isOpened.set(true))
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    // We do not explicitly close the server and instead let the cluster
    // deal with this as an unclean exit.
    return closeClient();
  }

  /**
   * Deletes the partition.
   *
   * @return future to be completed once the partition has been deleted
   */
  public CompletableFuture<Void> delete() {
    return closeServer().thenCompose(v -> closeClient()).thenRun(() -> deleteServer());
  }

  /**
   * Returns the partition data folder.
   *
   * @return the partition data folder
   */
  public abstract File getDataFolder();

  /**
   * Returns the partition name.
   *
   * @return the partition name
   */
  public abstract String getName();

  /**
   * Returns the identifier of the {@link Partition partition} associated with this instance.
   *
   * @return partition identifier
   */
  public PartitionId getId() {
    return partition.id();
  }

  /**
   * Returns the identifiers of partition members.
   *
   * @return partition member instance ids
   */
  public Collection<NodeId> getMembers() {
    return partition.members();
  }

  /**
   * Returns the {@link MemberId identifiers} of partition members.
   *
   * @return partition member identifiers
   */
  public Collection<MemberId> getMemberIds() {
    return Collections2.transform(getMembers(), n -> MemberId.from(n.id()));
  }

  /**
   * Attempts to rejoin the partition.
   *
   * @return future that is completed after the operation is complete
   */
  protected abstract CompletableFuture<Void> openServer();

  /**
   * Attempts to join the partition as a new member.
   *
   * @return future that is completed after the operation is complete
   */
  private CompletableFuture<Void> joinCluster() {
    Set<NodeId> otherMembers = partition.members()
        .stream()
        .filter(nodeId -> !nodeId.equals(localNodeId))
        .collect(Collectors.toSet());
    AtomixPartitionServer server = new AtomixPartitionServer(this,
        MemberId.from(localNodeId.id()),
        clusterCommunicator);
    return server.join(Collections2.transform(otherMembers, n -> MemberId.from(n.id())))
        .thenRun(() -> this.server = server);
  }

  private CompletableFuture<AtomixPartitionClient> openClient() {
    client = new AtomixPartitionClient(this,
        MemberId.from(localNodeId.id()),
        new RaftClientCommunicator(
            String.format("partition-%d", partition.id().id()),
            Serializer.using(StorageNamespaces.RAFT_PROTOCOL),
            clusterCommunicator));
    return client.open().thenApply(v -> client);
  }

  /**
   * Closes the partition server if it was previously opened.
   *
   * @return future that is completed when the operation completes
   */
  public CompletableFuture<Void> leaveCluster() {
    return server != null
        ? server.closeAndExit().thenRun(() -> server.delete())
        : CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return isOpened.get();
  }

  private CompletableFuture<Void> closeServer() {
    if (server != null) {
      return server.close();
    }
    return CompletableFuture.completedFuture(null);
  }

  private void deleteServer() {
    if (server != null) {
      server.delete();
    }
  }

  private CompletableFuture<Void> closeClient() {
    if (client != null) {
      return client.close();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Process updates to partitions and handles joining or leaving a partition.
   *
   * @param newValue new Partition
   */
  void onUpdate(Partition newValue) {
    boolean wasPresent = partition.members().contains(localNodeId);
    boolean isPresent = newValue.members().contains(localNodeId);
    this.partition = newValue;
    if ((wasPresent && isPresent) || (!wasPresent && !isPresent)) {
      // no action needed
      return;
    }
    // Only need to do action if our membership changed
    if (wasPresent) {
      leaveCluster();
    } else if (isPresent) {
      joinCluster();
    }
  }
}
