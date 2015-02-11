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

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.util.internal.Quorum;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class FollowerState extends ActiveState {
  private final Random random = new Random();
  private ScheduledFuture<?> currentTimer;

  public FollowerState(RaftContext context) {
    super(context);
  }

  @Override
  public Type type() {
    return Type.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open().thenRun(this::startHeartbeatTimeout);
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimeout() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getLocalMember().uri());
    resetHeartbeatTimeout();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimeout() {
    context.checkThread();
    if (isClosed()) return;

    // If a timer is already set, cancel the timer.
    if (currentTimer != null) {
      LOGGER.debug("{} - Reset heartbeat timeout", context.getLocalMember().uri());
      currentTimer.cancel(false);
    }

    // Set the election timeout in a semi-random fashion with the random range
    // being election timeout and 2 * election timeout.
    long delay = context.getElectionTimeout() + (random.nextInt((int) context.getElectionTimeout()) % context.getElectionTimeout());
    currentTimer = context.executor().schedule(() -> {
      currentTimer = null;
      if (isOpen()) {
        if (context.getLastVotedFor() == null) {
          LOGGER.info("{} - Heartbeat timed out in {} milliseconds", context.getLocalMember().uri(), delay);
          sendPollRequests();
        } else {
          // If the node voted for a candidate then reset the election timer.
          resetHeartbeatTimeout();
        }
      }
    }, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Polls all members of the cluster to determine whether this member should transition to the CANDIDATE state.
   */
  private void sendPollRequests() {
    // Set a new timer within which other nodes must respond in order for this node to transition to candidate.
    currentTimer = context.executor().schedule(() -> {
      LOGGER.debug("{} - Failed to poll a majority of the cluster in {} milliseconds", context.getLocalMember().uri(), context.getElectionTimeout());
      resetHeartbeatTimeout();
    }, context.getElectionTimeout(), TimeUnit.MILLISECONDS);

    // Create a quorum that will track the number of nodes that have responded to the poll request.
    final AtomicBoolean complete = new AtomicBoolean();
    final Set<RaftMember> votingMembers = context.getMembers().stream()
      .filter(m -> m.type() == Member.Type.ACTIVE)
      .collect(Collectors.toSet());

    final Quorum quorum = new Quorum((int) Math.ceil(votingMembers.size() / 2.0), (elected) -> {
      // If a majority of the cluster indicated they would vote for us then transition to candidate.
      complete.set(true);
      if (elected) {
        transition(Type.CANDIDATE);
      } else {
        resetHeartbeatTimeout();
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    Long lastIndex = context.log().lastIndex();
    ByteBuffer lastEntry = lastIndex != null ? context.log().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    LOGGER.info("{} - Polling members {}", context.getLocalMember().uri(), votingMembers);
    final Long lastTerm = lastEntry != null ? lastEntry.getLong() : null;
    for (RaftMember member : votingMembers) {
      LOGGER.debug("{} - Polling {} for next term {}", context.getLocalMember().uri(), member, context.getTerm() + 1);
      PollRequest request = PollRequest.builder()
        .withUri(member.uri())
        .withTerm(context.getTerm())
        .withCandidate(context.getLocalMember().uri())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      pollHandler.apply(request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        if (isOpen() && !complete.get()) {
          if (error != null) {
            LOGGER.warn(context.getLocalMember().uri(), error);
            quorum.fail();
          } else {
            if (response.term() > context.getTerm()) {
              context.setTerm(response.term());
            }
            if (!response.accepted()) {
              LOGGER.info("{} - Received rejected poll from {}", context.getLocalMember().uri(), member);
              quorum.fail();
            } else if (response.term() != context.getTerm()) {
              LOGGER.info("{} - Received accepted poll for a different term from {}", context.getLocalMember().uri(), member);
              quorum.fail();
            } else {
              LOGGER.info("{} - Received accepted poll from {}", context.getLocalMember().uri(), member);
              quorum.succeed();
            }
          }
        }
      }, context.executor());
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    resetHeartbeatTimeout();
    CompletableFuture<AppendResponse> response = super.append(request);
    resetHeartbeatTimeout();
    return response;
  }

  @Override
  protected VoteResponse handleVote(VoteRequest request) {
    // Reset the heartbeat timeout if we voted for another candidate.
    VoteResponse response = super.handleVote(request);
    if (response.voted()) {
      resetHeartbeatTimeout();
    }
    return response;
  }

  /**
   * Cancels the heartbeat timeout.
   */
  private void cancelHeartbeatTimeout() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getLocalMember().uri());
      currentTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelHeartbeatTimeout);
  }

}
