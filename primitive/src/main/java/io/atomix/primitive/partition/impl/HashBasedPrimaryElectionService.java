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
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.utils.event.AbstractListenerManager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Hash-based primary election service.
 */
public class HashBasedPrimaryElectionService
    extends AbstractListenerManager<PrimaryElectionEvent, PrimaryElectionEventListener>
    implements ManagedPrimaryElectionService {

  private final ClusterService clusterService;
  private final Map<PartitionId, PrimaryElection> elections = Maps.newConcurrentMap();
  private final PrimaryElectionEventListener primaryElectionListener = this::post;
  private final AtomicBoolean started = new AtomicBoolean();

  public HashBasedPrimaryElectionService(ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  @Override
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> {
      PrimaryElection election = new HashBasedPrimaryElection(partitionId, clusterService);
      election.addListener(primaryElectionListener);
      return election;
    });
  }

  @Override
  public CompletableFuture<PrimaryElectionService> start() {
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
