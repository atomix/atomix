// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import com.google.common.collect.Sets;
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
  private final PartitionId partitionId;
  private long counter;
  private PrimaryTerm term;
  private final List<GroupMember> candidates = new ArrayList<>();
  private final Set<PrimaryElectionEventListener> listeners = Sets.newConcurrentHashSet();

  public TestPrimaryElection(PartitionId partitionId) {
    this.partitionId = partitionId;
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
    candidates.add(member);
    if (term == null) {
      term = new PrimaryTerm(++counter, member, Collections.emptyList());
      listeners.forEach(l -> l.event(new PrimaryElectionEvent(Type.CHANGED, partitionId, term)));
    } else {
      term = new PrimaryTerm(term.term(), term.primary(), candidates.stream()
          .filter(candidate -> !candidate.equals(term.primary()))
          .collect(Collectors.toList()));
      listeners.forEach(l -> l.event(new PrimaryElectionEvent(Type.CHANGED, partitionId, term)));
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
}
