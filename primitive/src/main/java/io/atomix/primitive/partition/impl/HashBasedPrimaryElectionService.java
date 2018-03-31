/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hash-based primary election service.
 */
public class HashBasedPrimaryElectionService
    extends AbstractListenerManager<PrimaryElectionEvent, PrimaryElectionEventListener>
    implements ManagedPrimaryElectionService {

  private static final String SUBJECT = "primary-election-counter";
  private static final long BROADCAST_INTERVAL = 5000;

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(NodeId.class)
      .build());

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final ClusterService clusterService;
  private final ClusterMessagingService messagingService;
  private final Map<PartitionId, PrimaryElection> elections = Maps.newConcurrentMap();
  private final PrimaryElectionEventListener primaryElectionListener = this::post;
  private final Map<NodeId, Integer> counters = Maps.newConcurrentMap();
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();
  private ScheduledFuture<?> broadcastFuture;

  public HashBasedPrimaryElectionService(ClusterService clusterService, ClusterMessagingService messagingService) {
    this.clusterService = clusterService;
    this.messagingService = messagingService;
    this.executor = Executors.newSingleThreadScheduledExecutor(Threads.namedThreads("primary-election-%d", log));
  }

  @Override
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> {
      PrimaryElection election = new HashBasedPrimaryElection(partitionId, clusterService, this);
      election.addListener(primaryElectionListener);
      return election;
    });
  }

  /**
   * Returns the current term.
   *
   * @return the current term
   */
  long getTerm() {
    return counters.values().stream().mapToInt(v -> v).sum();
  }

  /**
   * Increments and returns the current term.
   *
   * @return the current term
   */
  long incrementTerm() {
    counters.compute(clusterService.getLocalNode().id(), (id, value) -> value != null ? value + 1 : 1);
    broadcastCounters();
    return getTerm();
  }

  private void updateCounters(Map<NodeId, Integer> counters) {
    for (Map.Entry<NodeId, Integer> entry : counters.entrySet()) {
      this.counters.compute(entry.getKey(), (key, value) -> {
        if (value == null || value < entry.getValue()) {
          return entry.getValue();
        }
        return value;
      });
    }
  }

  private void broadcastCounters() {
    messagingService.broadcast(SUBJECT, counters, SERIALIZER::encode);
  }

  @Override
  public CompletableFuture<PrimaryElectionService> start() {
    if (started.compareAndSet(false, true)) {
      messagingService.subscribe(SUBJECT, SERIALIZER::decode, this::updateCounters, executor);
      broadcastFuture = executor.scheduleAtFixedRate(this::broadcastCounters, BROADCAST_INTERVAL, BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      messagingService.unsubscribe(SUBJECT);
      if (broadcastFuture != null) {
        broadcastFuture.cancel(false);
      }
    }
    return CompletableFuture.completedFuture(null);
  }
}
