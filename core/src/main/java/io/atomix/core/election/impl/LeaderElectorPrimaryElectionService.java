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
package io.atomix.core.election.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.NodeId;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leader elector based primary election service.
 */
public class LeaderElectorPrimaryElectionService implements ManagedPrimaryElectionService {
  private static final String PRIMITIVE_NAME = "atomix-primary-elector";
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(NodeId.class)
      .build());

  private final PartitionGroup partitions;
  private final Set<PrimaryElectionEventListener> listeners = Sets.newIdentityHashSet();
  private final PrimaryElectionEventListener eventListener = event -> listeners.forEach(l -> l.onEvent(event));
  private final Map<PartitionId, PrimaryElection> elections = Maps.newConcurrentMap();
  private AsyncLeaderElector<NodeId> elector;
  private final AtomicBoolean started = new AtomicBoolean();

  public LeaderElectorPrimaryElectionService(PartitionGroup partitionGroup) {
    this.partitions = checkNotNull(partitionGroup);
  }

  @Override
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> {
      PrimaryElection election = new LeaderElectorPrimaryElection(partitionId, elector);
      election.addListener(eventListener);
      return election;
    });
  }

  @Override
  public void addListener(PrimaryElectionEventListener listener) {
    listeners.add(checkNotNull(listener));
  }

  @Override
  public void removeListener(PrimaryElectionEventListener listener) {
    listeners.remove(checkNotNull(listener));
  }

  @SuppressWarnings("unchecked")
  private AsyncLeaderElector<NodeId> newLeaderElector(Partition partition) {
    PrimitiveProxy proxy = partition.getPrimitiveClient()
        .newProxy(PRIMITIVE_NAME, LeaderElectorType.instance(), RaftProtocol.builder()
            .withMinTimeout(Duration.ofMillis(250))
            .withMaxTimeout(Duration.ofSeconds(5))
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withRecoveryStrategy(Recovery.RECOVER)
            .withMaxRetries(5)
            .build());
    AsyncLeaderElector<byte[]> leaderElector = new LeaderElectorProxy(proxy.connect().join());
    return new TranscodingAsyncLeaderElector<>(leaderElector, SERIALIZER::encode, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<PrimaryElectionService> start() {
    Map<PartitionId, AsyncLeaderElector<NodeId>> electors = Maps.newConcurrentMap();
    for (Partition partition : partitions.getPartitions()) {
      electors.put(partition.id(), newLeaderElector(partition));
    }

    Partitioner<String> partitioner = topic -> partitions.getPartition(topic).id();
    elector = new PartitionedAsyncLeaderElector<>(PRIMITIVE_NAME, electors, partitioner);
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    elector.close();
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
