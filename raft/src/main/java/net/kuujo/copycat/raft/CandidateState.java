/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.AppendRequest;
import net.kuujo.copycat.raft.protocol.AppendResponse;
import net.kuujo.copycat.raft.protocol.VoteRequest;
import net.kuujo.copycat.raft.protocol.VoteResponse;
import net.kuujo.copycat.raft.util.Quorum;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Candidate status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class CandidateState extends ActiveState {
  private final Random random = new Random();
  private Quorum quorum;
  private ScheduledFuture<?> currentTimer;

  public CandidateState(RaftContext context) {
    super(context);
  }

  @Override
  public Type type() {
    return Type.CANDIDATE;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open().thenRun(this::startElection);
  }

  /**
   * Starts the election.
   */
  private void startElection() {
    LOGGER.info("{} - Starting election", context.getLocalMember().uri());
    sendVoteRequests();
  }

  /**
   * Resets the election timer.
   */
  private void sendVoteRequests() {
    context.checkThread();

    // Because of asynchronous execution, the candidate status could have already been closed. In that case,
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
    currentTimer = context.executor().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.info("{} - Election timed out", context.getLocalMember().uri());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      sendVoteRequests();
      LOGGER.info("{} - Restarted election", context.getLocalMember().uri());
    }, delay, TimeUnit.MILLISECONDS);

    final AtomicBoolean complete = new AtomicBoolean();
    final Set<RaftMember> votingMembers = context.getMembers().stream()
      .filter(m -> m.type() == RaftMember.Type.ACTIVE)
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
    Long lastIndex = context.log().lastIndex();
    ByteBuffer lastEntry = lastIndex != null ? context.log().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    LOGGER.info("{} - Requesting votes from {}", context.getLocalMember().uri(), votingMembers);
    final Long lastTerm = lastEntry != null ? lastEntry.getLong() : null;
    for (RaftMember member : votingMembers) {
      LOGGER.debug("{} - Requesting vote from {} for term {}", context.getLocalMember().uri(), member, context.getTerm());
      VoteRequest request = VoteRequest.builder()
        .withUri(member.uri())
        .withTerm(context.getTerm())
        .withCandidate(context.getLocalMember().uri())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      voteHandler.apply(request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        if (isOpen() && !complete.get()) {
          if (error != null) {
            LOGGER.warn(context.getLocalMember().uri(), error);
            quorum.fail();
          } else if (response.term() > context.getTerm()) {
            LOGGER.debug("{} - Received greater term from {}", context.getLocalMember().uri(), member);
            context.setTerm(response.term());
            complete.set(true);
            transition(RaftState.Type.FOLLOWER);
          } else if (!response.voted()) {
            LOGGER.info("{} - Received rejected vote from {}", context.getLocalMember().uri(), member);
            quorum.fail();
          } else if (response.term() != context.getTerm()) {
            LOGGER.info("{} - Received successful vote for a different term from {}", context.getLocalMember().uri(), member);
            quorum.fail();
          } else {
            LOGGER.info("{} - Received successful vote from {}", context.getLocalMember().uri(), member);
            quorum.succeed();
          }
        }
      }, context.executor());
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
    if (request.candidate().equals(context.getLocalMember().uri())) {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .withVoted(true)
        .build()));
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withUri(context.getLocalMember().uri())
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
      LOGGER.debug("{} - Cancelling election", context.getLocalMember().uri());
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
