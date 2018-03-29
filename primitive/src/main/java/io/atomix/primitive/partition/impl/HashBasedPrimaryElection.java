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

import com.google.common.hash.Hashing;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.utils.event.AbstractListenerManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Hash-based primary election.
 */
public class HashBasedPrimaryElection
    extends AbstractListenerManager<PrimaryElectionEvent, PrimaryElectionEventListener>
    implements PrimaryElection {

  private final PartitionId partitionId;
  private final ClusterService clusterService;
  private final ClusterEventListener clusterEventListener = e -> recomputeTerm();
  private volatile PrimaryTerm currentTerm;

  public HashBasedPrimaryElection(PartitionId partitionId, ClusterService clusterService) {
    this.partitionId = partitionId;
    this.clusterService = clusterService;
    recomputeTerm();
    clusterService.addListener(clusterEventListener);
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(NodeId nodeId) {
    return CompletableFuture.completedFuture(currentTerm);
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return CompletableFuture.completedFuture(currentTerm);
  }

  /**
   * Recomputes the current term.
   */
  private void recomputeTerm() {
    List<NodeId> candidates = new ArrayList<>();
    for (Node node : clusterService.getNodes()) {
      if (node.getState() == Node.State.ACTIVE) {
        candidates.add(node.id());
      }
    }
    Collections.sort(candidates, (a, b) -> {
      int aoffset = Hashing.murmur3_32().hashString(a.id(), StandardCharsets.UTF_8).asInt() % partitionId.id();
      int boffset = Hashing.murmur3_32().hashString(b.id(), StandardCharsets.UTF_8).asInt() % partitionId.id();
      return aoffset - boffset;
    });
    currentTerm = new PrimaryTerm(System.currentTimeMillis(), candidates.get(0), candidates.subList(1, candidates.size()));
  }

  @Override
  public CompletableFuture<Void> open() {
    clusterService.addListener(clusterEventListener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    clusterService.removeListener(clusterEventListener);
  }
}
