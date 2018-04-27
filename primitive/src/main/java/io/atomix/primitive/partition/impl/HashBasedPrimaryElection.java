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
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupId;
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
  private final ClusterMembershipService clusterMembershipService;
  private final HashBasedPrimaryElectionService electionService;
  private final ClusterMembershipEventListener membershipEventListener = e -> recomputeTerm();
  private volatile PrimaryTerm currentTerm;

  public HashBasedPrimaryElection(PartitionId partitionId, ClusterMembershipService clusterMembershipService, HashBasedPrimaryElectionService electionService) {
    this.partitionId = partitionId;
    this.clusterMembershipService = clusterMembershipService;
    this.electionService = electionService;
    recomputeTerm();
    clusterMembershipService.addListener(membershipEventListener);
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
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
    List<GroupMember> candidates = new ArrayList<>();
    for (Member member : clusterMembershipService.getMembers()) {
      if (member.getState() == Member.State.ACTIVE) {
        candidates.add(new GroupMember(member.id(), MemberGroupId.from(member.id().id())));
      }
    }
    candidates.sort((a, b) -> {
      int aoffset = Hashing.murmur3_32().hashString(a.memberId().id(), StandardCharsets.UTF_8).asInt() % partitionId.id();
      int boffset = Hashing.murmur3_32().hashString(b.memberId().id(), StandardCharsets.UTF_8).asInt() % partitionId.id();
      return aoffset - boffset;
    });
    currentTerm = new PrimaryTerm(
        electionService.incrementTerm(),
        candidates.isEmpty() ? null : candidates.get(0),
        candidates.isEmpty() ? Collections.emptyList() : candidates.subList(1, candidates.size()));
    post(new PrimaryElectionEvent(PrimaryElectionEvent.Type.CHANGED, partitionId, currentTerm));
  }
}
