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
package io.atomix.primitive.partition;

import com.google.common.collect.Sets;
import io.atomix.cluster.NodeId;
import io.atomix.primitive.partition.PrimaryElectionEvent.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Test primary election.
 */
public class TestPrimaryElection implements PrimaryElection {
  private long counter;
  private PrimaryTerm term;
  private final List<NodeId> candidates = new ArrayList<>();
  private final Set<PrimaryElectionEventListener> listeners = Sets.newConcurrentHashSet();

  @Override
  public CompletableFuture<PrimaryTerm> enter(NodeId nodeId) {
    candidates.add(nodeId);
    if (term == null) {
      term = new PrimaryTerm(++counter, nodeId, Collections.emptyList());
      listeners.forEach(l -> l.onEvent(new PrimaryElectionEvent(Type.PRIMARY_AND_BACKUPS_CHANGED, term)));
    } else {
      term = new PrimaryTerm(term.term(), term.primary(), candidates.stream()
          .filter(candidate -> !candidate.equals(term.primary()))
          .collect(Collectors.toList()));
      listeners.forEach(l -> l.onEvent(new PrimaryElectionEvent(Type.BACKUPS_CHANGED, term)));
    }
    return CompletableFuture.completedFuture(term);
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return CompletableFuture.completedFuture(term);
  }

  @Override
  public void addListener(PrimaryElectionEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(PrimaryElectionEventListener listener) {
    listeners.remove(listener);
  }

  @Override
  public void close() {

  }
}
