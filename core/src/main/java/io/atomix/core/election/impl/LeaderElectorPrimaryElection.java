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

import com.google.common.collect.Sets;
import io.atomix.cluster.NodeId;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEvent.Type;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leader elector based primary election.
 */
public class LeaderElectorPrimaryElection implements PrimaryElection {
  private final PartitionId partitionId;
  private final AsyncLeaderElector<NodeId> elector;
  private final LeadershipEventListener<NodeId> eventListener = event -> updateTerm(event.newLeadership());
  private final Set<PrimaryElectionEventListener> listeners = Sets.newCopyOnWriteArraySet();
  private PrimaryTerm term;

  public LeaderElectorPrimaryElection(PartitionId partitionId, AsyncLeaderElector<NodeId> elector) {
    this.partitionId = checkNotNull(partitionId);
    this.elector = checkNotNull(elector);
    elector.addListener(eventListener);
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(NodeId nodeId) {
    return elector.run(partitionId.toString(), nodeId).thenApply(this::createTerm);
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return elector.getLeadership(partitionId.toString()).thenApply(this::createTerm);
  }

  private PrimaryTerm createTerm(Leadership<NodeId> leadership) {
    NodeId leader = leadership.leader() != null ? leadership.leader().id() : null;
    return new PrimaryTerm(
        leadership.leader() != null ? leadership.leader().term() : 0,
        leader,
        leadership.candidates()
            .stream()
            .filter(candidate -> !Objects.equals(candidate, leader))
            .collect(Collectors.toList()));
  }

  private PrimaryTerm updateTerm(Leadership<NodeId> leadership) {
    PrimaryTerm oldTerm = this.term;
    PrimaryTerm term = createTerm(leadership);

    if (oldTerm != null) {
      PrimaryElectionEvent.Type type = null;
      if (!Objects.equals(oldTerm.primary(), term.primary())) {
        if (!Objects.equals(oldTerm.backups(), term.backups())) {
          type = Type.PRIMARY_AND_BACKUPS_CHANGED;
        } else {
          type = Type.PRIMARY_CHANGED;
        }
      } else if (!Objects.equals(oldTerm.backups(), term.backups())) {
        type = Type.BACKUPS_CHANGED;
      }

      if (type != null) {
        this.term = term;
        PrimaryElectionEvent event = new PrimaryElectionEvent(type, term);
        listeners.forEach(l -> l.onEvent(event));
      }
    } else {
      this.term = term;
    }
    return this.term;
  }

  @Override
  public void addListener(PrimaryElectionEventListener listener) {
    listeners.add(checkNotNull(listener));
  }

  @Override
  public void removeListener(PrimaryElectionEventListener listener) {
    listeners.remove(checkNotNull(listener));
  }

  @Override
  public void close() {
    elector.removeListener(eventListener);
  }
}
