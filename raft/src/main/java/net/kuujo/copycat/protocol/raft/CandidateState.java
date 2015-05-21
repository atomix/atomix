/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol.raft;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.raft.rpc.*;
import net.kuujo.copycat.protocol.raft.storage.RaftEntry;
import net.kuujo.copycat.protocol.raft.util.Quorum;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Candidate state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class CandidateState extends ActiveState {
  private final Random random = new Random();
  private Quorum quorum;
  private ScheduledFuture<?> currentTimer;

  public CandidateState(Raft context) {
    super(context);
  }

  @Override
  public RaftState.Type type() {
    return RaftState.Type.CANDIDATE;
  }

  @Override
  public synchronized CompletableFuture<RaftState> open() {
    return super.open().thenRun(this::startElection).thenApply(v -> this);
  }

  /**
   * Starts the election.
   */
  private void startElection() {
    LOGGER.info("{} - Starting election", context.cluster().member().id());
    sendVoteRequests();
  }

  /**
   * Resets the election timer.
   */
  private void sendVoteRequests() {
    context.checkThread();

    // Because of asynchronous execution, the candidate state could have already been closed. In that case,
    // simply skip the election.
    if (isClosed()) return;

    // Cancel the current timer task and purge the election timer of cancelled tasks.
    if (currentTimer != null) {
      currentTimer.cancel(false);
    }

    // When the election timer is reset, increment the current term and
    // restart the election.
    context.setTerm(context.getTerm() + 1);

    long delay = context.getElectionTimeout() + (random.nextInt((int) context.getElectionTimeout()) % context.getElectionTimeout());
    currentTimer = context.context().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.info("{} - Election timed out", context.cluster().member().id());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      sendVoteRequests();
      LOGGER.info("{} - Restarted election", context.cluster().member().id());
    }, delay, TimeUnit.MILLISECONDS);

    final AtomicBoolean complete = new AtomicBoolean();
    final Set<Member> votingMembers = context.cluster().members().stream()
      .filter(m -> m.type() == Member.Type.ACTIVE)
      .collect(Collectors.toSet());

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum((int) Math.ceil(votingMembers.size() / 2.0), (elected) -> {
      complete.set(true);
      if (elected) {
        transition(RaftState.Type.LEADER);
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    long lastIndex = context.log().lastIndex();
    RaftEntry lastEntry = lastIndex != 0 ? context.log().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    LOGGER.info("{} - Requesting votes from {}", context.cluster().member().id(), votingMembers);
    final long lastTerm = lastEntry != null ? lastEntry.getTerm() : 0;
    for (Member member : votingMembers) {
      LOGGER.debug("{} - Requesting vote from {} for term {}", context.cluster().member().id(), member, context.getTerm());
      VoteRequest request = VoteRequest.builder()
        .withTerm(context.getTerm())
        .withCandidate(context.cluster().member().id())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      member.<VoteRequest, VoteResponse>send(context.topic(), request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        if (isOpen() && !complete.get()) {
          if (error != null) {
            LOGGER.warn(error.getMessage());
            quorum.fail();
          } else if (response.term() > context.getTerm()) {
            LOGGER.debug("{} - Received greater term from {}", context.cluster().member().id(), member);
            context.setTerm(response.term());
            complete.set(true);
            transition(RaftState.Type.FOLLOWER);
          } else if (!response.voted()) {
            LOGGER.info("{} - Received rejected vote from {}", context.cluster().member().id(), member);
            quorum.fail();
          } else if (response.term() != context.getTerm()) {
            LOGGER.info("{} - Received successful vote for a different term from {}", context.cluster().member().id(), member);
            quorum.fail();
          } else {
            LOGGER.info("{} - Received successful vote from {}", context.cluster().member().id(), member);
            quorum.succeed();
          }
        }
      }, context.context());
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() >= context.getTerm()) {
      context.setTerm(request.term());
      transition(RaftState.Type.FOLLOWER);
    }
    return super.append(request);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
      transition(RaftState.Type.FOLLOWER);
      return super.vote(request);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (request.candidate() == context.cluster().member().id()) {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(true)
        .build()));
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Response.Status.OK)
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
      LOGGER.debug("{} - Cancelling election", context.cluster().member().id());
      currentTimer.cancel(false);
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
