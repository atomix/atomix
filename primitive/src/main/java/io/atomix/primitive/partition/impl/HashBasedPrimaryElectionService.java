// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.event.AbstractListenerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hash-based primary election service.
 */
public class HashBasedPrimaryElectionService
    extends AbstractListenerManager<PrimaryElectionEvent, PrimaryElectionEventListener>
    implements ManagedPrimaryElectionService {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final ClusterMembershipService clusterMembershipService;
  private final PartitionGroupMembershipService groupMembershipService;
  private final ClusterCommunicationService messagingService;
  private final Map<PartitionId, HashBasedPrimaryElection> elections = Maps.newConcurrentMap();
  private final PrimaryElectionEventListener primaryElectionListener = this::post;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();

  public HashBasedPrimaryElectionService(ClusterMembershipService clusterMembershipService, PartitionGroupMembershipService groupMembershipService, ClusterCommunicationService messagingService) {
    this.clusterMembershipService = clusterMembershipService;
    this.groupMembershipService = groupMembershipService;
    this.messagingService = messagingService;
    this.executor = Executors.newSingleThreadScheduledExecutor(Threads.namedThreads("primary-election-%d", log));
  }

  @Override
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> {
      HashBasedPrimaryElection election = new HashBasedPrimaryElection(partitionId, clusterMembershipService, groupMembershipService, messagingService, executor);
      election.addListener(primaryElectionListener);
      return election;
    });
  }

  @Override
  public CompletableFuture<PrimaryElectionService> start() {
    started.set(true);
    log.info("Started");
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      elections.values().forEach(election -> election.close());
    }
    executor.shutdownNow();
    return CompletableFuture.completedFuture(null);
  }
}
