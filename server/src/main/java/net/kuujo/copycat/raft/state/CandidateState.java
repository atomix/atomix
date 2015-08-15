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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.raft.protocol.request.AppendRequest;
import net.kuujo.copycat.raft.protocol.request.VoteRequest;
import net.kuujo.copycat.raft.protocol.response.AppendResponse;
import net.kuujo.copycat.raft.protocol.response.Response;
import net.kuujo.copycat.raft.protocol.response.VoteResponse;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.raft.storage.RaftEntry;
import net.kuujo.copycat.raft.util.Quorum;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Candidate state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class CandidateState extends ActiveState {
  private final Random random = new Random();
  private Quorum quorum;
  private ScheduledFuture<?> currentTimer;

  public CandidateState(ServerContext context) {
    super(context);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.CANDIDATE;
  }

  @Override
  public synchronized CompletableFuture<AbstractState> open() {
    return super.open().thenRun(this::startElection).thenApply(v -> this);
  }

  /**
   * Starts the election.
   */
  private void startElection() {
    LOGGER.info("{} - Starting election", context.getMember().id());
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

    long delay = context.getElectionTimeout().toMillis() + (random.nextInt((int) context.getElectionTimeout().toMillis()) % context.getElectionTimeout().toMillis());
    currentTimer = context.getContext().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.debug("{} - Election timed out", context.getMember().id());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      sendVoteRequests();
      LOGGER.debug("{} - Restarted election", context.getMember().id());
    }, delay, TimeUnit.MILLISECONDS);

    final AtomicBoolean complete = new AtomicBoolean();
    final Set<MemberState> votingMembers = new HashSet<>(context.getCluster().getActiveMembers());

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum(context.getCluster().getQuorum(), (elected) -> {
      complete.set(true);
      if (elected) {
        transition(RaftServer.State.LEADER);
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    long lastIndex = context.getLog().lastIndex();
    RaftEntry lastEntry = lastIndex != 0 ? context.getLog().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    LOGGER.debug("{} - Requesting votes from {}", context.getMember().id(), votingMembers);
    final long lastTerm = lastEntry != null ? lastEntry.getTerm() : 0;
    for (MemberState member : votingMembers) {
      LOGGER.debug("{} - Requesting vote from {} for term {}", context.getMember().id(), member, context.getTerm());
      VoteRequest request = VoteRequest.builder()
        .withTerm(context.getTerm())
        .withCandidate(context.getMember().id())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();

      context.getConnections().getConnection(member.getMember()).thenAccept(connection -> {
        connection.<VoteRequest, VoteResponse>send(request).whenCompleteAsync((response, error) -> {
          context.checkThread();
          if (isOpen() && !complete.get()) {
            if (error != null) {
              LOGGER.warn(error.getMessage());
              quorum.fail();
            } else if (response.term() > context.getTerm()) {
              LOGGER.debug("{} - Received greater term from {}", context.getMember().id(), member);
              context.setTerm(response.term());
              complete.set(true);
              transition(RaftServer.State.FOLLOWER);
            } else if (!response.voted()) {
              LOGGER.debug("{} - Received rejected vote from {}", context.getMember().id(), member);
              quorum.fail();
            } else if (response.term() != context.getTerm()) {
              LOGGER.debug("{} - Received successful vote for a different term from {}", context.getMember().id(), member);
              quorum.fail();
            } else {
              LOGGER.debug("{} - Received successful vote from {}", context.getMember().id(), member);
              quorum.succeed();
            }
          }
        }, context.getContext());
      });
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() >= context.getTerm()) {
      context.setTerm(request.term());
      transition(RaftServer.State.FOLLOWER);
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
      transition(RaftServer.State.FOLLOWER);
      return super.vote(request);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (request.candidate() == context.getMember().id()) {
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
      LOGGER.debug("{} - Cancelling election", context.getMember().id());
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
