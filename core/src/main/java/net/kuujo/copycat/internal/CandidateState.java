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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.internal.util.Quorum;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(CandidateState.class);
  private final Random random = new Random();
  private Quorum quorum;
  private ScheduledFuture<?> currentTimer;

  CandidateState(CopycatStateContext context) {
    super(context);
  }

  @Override
  public CopycatState state() {
    return CopycatState.CANDIDATE;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenRunAsync(this::startElection, context.executor());
  }

  /**
   * Starts the election.
   */
  private void startElection() {
    LOGGER.info("{} - Starting election", context.getLocalMember());
    resetTimer();
  }

  /**
   * Requests votes from a majority of the cluster.
   */
  private void requestVotes() {

  }

  /**
   * Resets the election timer.
   */
  private void resetTimer() {
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
    currentTimer = context.executor().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.info("{} - Election timed out", context.getLocalMember());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      resetTimer();
      LOGGER.info("{} - Restarted election", context.getLocalMember());
    }, delay, TimeUnit.MILLISECONDS);

    final AtomicBoolean complete = new AtomicBoolean();

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum((int) Math.floor(context.getReplicas().size() / 2) + 1, (elected) -> {
      complete.set(true);
      if (elected) {
        transition(CopycatState.LEADER);
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    final Long lastIndex = context.log().lastIndex();
    ByteBuffer lastEntry = lastIndex != null ? context.log().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and poll each member for a vote.
    LOGGER.info("{} - Polling members {}", context.getLocalMember(), context.getReplicas());
    final Long lastTerm = lastEntry != null ? lastEntry.getLong() : null;
    for (String member : context.getReplicas()) {
      LOGGER.debug("{} - Polling {}", context.getLocalMember(), member);
      PollRequest request = PollRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withUri(member)
        .withTerm(context.getTerm())
        .withCandidate(context.getLocalMember())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      pollHandler.handle(request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        if (isOpen() && !complete.get()) {
          if (error != null) {
            LOGGER.warn(context.getLocalMember(), error);
            quorum.fail();
          } else if (!response.voted()) {
            LOGGER.info("{} - Received rejected vote from {}", context.getLocalMember(), member);
            quorum.fail();
          } else if (response.term() != context.getTerm()) {
            LOGGER.info("{} - Received successful vote for a different term from {}", context.getLocalMember(), member);
            quorum.fail();
          } else {
            LOGGER.info("{} - Received successful vote from {}", context.getLocalMember(), member);
            quorum.succeed();
          }
        }
      }, context.executor());
    }
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() >= context.getTerm()) {
      context.setTerm(request.term());
      transition(CopycatState.FOLLOWER);
    }
    return super.ping(request);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
      transition(CopycatState.FOLLOWER);
      return super.poll(request);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (request.candidate().equals(context.getLocalMember())) {
      return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
        .withId(logRequest(request).id())
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(true)
        .build()));
    } else {
      return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
        .withId(logRequest(request).id())
        .withUri(context.getLocalMember())
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
      LOGGER.debug("{} - Cancelling election", context.getLocalMember());
      currentTimer.cancel(false);
    }
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRunAsync(this::cancelElection, context.executor());
  }

}
