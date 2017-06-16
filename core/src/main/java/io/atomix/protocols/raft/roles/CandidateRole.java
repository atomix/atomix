/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.utils.Quorum;
import io.atomix.storage.journal.Indexed;
import io.atomix.util.concurrent.Scheduled;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Candidate state.
 */
public final class CandidateRole extends ActiveRole {
  private final Random random = new Random();
  private Quorum quorum;
  private Scheduled currentTimer;

  public CandidateRole(RaftServerContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role type() {
    return RaftServer.Role.CANDIDATE;
  }

  @Override
  public synchronized CompletableFuture<RaftRole> open() {
    return super.open().thenRun(this::startElection).thenApply(v -> this);
  }

  /**
   * Starts the election.
   */
  void startElection() {
    LOGGER.info("{} - Starting election", context.getCluster().member().id());
    sendVoteRequests();
  }

  /**
   * Resets the election timer.
   */
  private void sendVoteRequests() {
    context.checkThread();

    // Because of asynchronous execution, the candidate state could have already been closed. In that case,
    // simply skip the election.
    if (isClosed()) {
      return;
    }

    // Cancel the current timer task and purge the election timer of cancelled tasks.
    if (currentTimer != null) {
      currentTimer.cancel();
    }

    // When the election timer is reset, increment the current term and
    // restart the election.
    context.setTerm(context.getTerm() + 1).setLastVotedFor(context.getCluster().member().id());

    Duration delay = context.getElectionTimeout().plus(Duration.ofMillis(random.nextInt((int) context.getElectionTimeout().toMillis())));
    currentTimer = context.getThreadContext().schedule(delay, () -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.debug("{} - Election timed out", context.getCluster().member().id());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      sendVoteRequests();
      LOGGER.debug("{} - Restarted election", context.getCluster().member().id());
    });

    final AtomicBoolean complete = new AtomicBoolean();
    final Set<DefaultRaftMember> votingMembers = new HashSet<>(context.getClusterState().getActiveMemberStates().stream().map(RaftMemberContext::getMember).collect(Collectors.toList()));

    // If there are no other members in the cluster, immediately transition to leader.
    if (votingMembers.isEmpty()) {
      LOGGER.trace("{} - Single member cluster. Transitioning directly to leader.", context.getCluster().member().id());
      context.transition(RaftServer.Role.LEADER);
      return;
    }

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum(context.getClusterState().getQuorum(), (elected) -> {
      complete.set(true);
      if (elected) {
        context.transition(RaftServer.Role.LEADER);
      } else {
        context.transition(RaftServer.Role.FOLLOWER);
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    final Indexed<RaftLogEntry> lastEntry = context.getLogWriter().lastEntry();

    final long lastTerm;
    if (lastEntry != null) {
      lastTerm = lastEntry.entry().term();
    } else {
      lastTerm = 0;
    }

    LOGGER.debug("{} - Requesting votes for term {}", context.getCluster().member().id(), context.getTerm());

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    for (DefaultRaftMember member : votingMembers) {
      LOGGER.debug("{} - Requesting vote from {} for term {}", context.getCluster().member().id(), member, context.getTerm());
      VoteRequest request = VoteRequest.builder()
          .withTerm(context.getTerm())
          .withCandidate(context.getCluster().member().id())
          .withLogIndex(lastEntry != null ? lastEntry.index() : 0)
          .withLogTerm(lastTerm)
          .build();

      context.getProtocolDispatcher().vote(member.id(), request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        if (isOpen() && !complete.get()) {
          if (error != null) {
            LOGGER.warn(error.getMessage());
            quorum.fail();
          } else {
            if (response.term() > context.getTerm()) {
              LOGGER.trace("{} - Received greater term from {}", context.getCluster().member().id(), member);
              context.setTerm(response.term());
              complete.set(true);
              context.transition(RaftServer.Role.FOLLOWER);
            } else if (!response.voted()) {
              LOGGER.trace("{} - Received rejected vote from {}", context.getCluster().member().id(), member);
              quorum.fail();
            } else if (response.term() != context.getTerm()) {
              LOGGER.trace("{} - Received successful vote for a different term from {}", context.getCluster().member().id(), member);
              quorum.fail();
            } else {
              LOGGER.trace("{} - Received successful vote from {}", context.getCluster().member().id(), member);
              quorum.succeed();
            }
          }
        }
      }, context.getThreadContext());
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() >= context.getTerm()) {
      context.setTerm(request.term());
      context.transition(RaftServer.Role.FOLLOWER);
    }
    return super.append(request);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (updateTermAndLeader(request.term(), null)) {
      CompletableFuture<VoteResponse> future = super.vote(request);
      context.transition(RaftServer.Role.FOLLOWER);
      return future;
    }

    // If the vote request is not for this candidate then reject the vote.
    if (request.candidate() == context.getCluster().member().id()) {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(true)
          .build()));
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build()));
    }
  }

  /**
   * Cancels the election.
   */
  private void cancelElection() {
    context.checkThread();
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling election", context.getCluster().member().id());
      currentTimer.cancel();
    }
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelElection);
  }

}
