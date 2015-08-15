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

import net.kuujo.copycat.raft.protocol.error.RaftError;
import net.kuujo.copycat.raft.protocol.request.*;
import net.kuujo.copycat.raft.protocol.response.*;
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
 * Follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class FollowerState extends ActiveState {
  private final Random random = new Random();
  private ScheduledFuture<?> heartbeatTimer;

  public FollowerState(ServerContext context) {
    super(context);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<AbstractState> open() {
    return super.open().thenRun(this::startHeartbeatTimeout).thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getConnections()
        .getConnection(context.getLeader())
        .thenCompose(connection -> connection.send(request));
    }
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getConnections()
        .getConnection(context.getLeader())
        .thenCompose(connection -> connection.send(request));
    }
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimeout() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getMember().id());
    resetHeartbeatTimeout();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimeout() {
    context.checkThread();
    if (isClosed())
      return;

    // If a timer is already set, cancel the timer.
    if (heartbeatTimer != null) {
      LOGGER.debug("{} - Reset heartbeat timeout", context.getMember().id());
      heartbeatTimer.cancel(false);
    }

    // Set the election timeout in a semi-random fashion with the random range
    // being election timeout and 2 * election timeout.
    long delay = context.getElectionTimeout() + (random.nextInt((int) context.getElectionTimeout()) % context.getElectionTimeout());
    heartbeatTimer = context.getContext().schedule(() -> {
      heartbeatTimer = null;
      if (isOpen()) {
        if (context.getLastVotedFor() == 0) {
          LOGGER.debug("{} - Heartbeat timed out in {} milliseconds", context.getMember().id(), delay);
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
    heartbeatTimer = context.getContext().schedule(() -> {
      LOGGER.debug("{} - Failed to poll a majority of the cluster in {} milliseconds", context.getMember().id(), context.getElectionTimeout());
      resetHeartbeatTimeout();
    }, context.getElectionTimeout(), TimeUnit.MILLISECONDS);

    // Create a quorum that will track the number of nodes that have responded to the poll request.
    final AtomicBoolean complete = new AtomicBoolean();
    final Set<MemberState> votingMembers = new HashSet<>(context.getCluster().getActiveMembers());

    final Quorum quorum = new Quorum(context.getCluster().getQuorum(), (elected) -> {
      // If a majority of the cluster indicated they would vote for us then transition to candidate.
      complete.set(true);
      if (elected) {
        transition(RaftServer.State.CANDIDATE);
      } else {
        resetHeartbeatTimeout();
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    long lastIndex = context.getLog().lastIndex();
    RaftEntry lastEntry = context.getLog().containsEntry(lastIndex) ? context.getLog().getEntry(lastIndex) : null;

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    LOGGER.debug("{} - Polling members {}", context.getMember().id(), votingMembers);
    final long lastTerm = lastEntry != null ? lastEntry.getTerm() : 0;
    for (MemberState member : votingMembers) {
      LOGGER.debug("{} - Polling {} for next term {}", context.getMember().id(), member, context.getTerm() + 1);
      PollRequest request = PollRequest.builder()
        .withTerm(context.getTerm())
        .withCandidate(context.getMember().id())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      context.getConnections().getConnection(member.getMember()).thenAccept(connection -> {
        connection.<PollRequest, PollResponse>send(request).whenCompleteAsync((response, error) -> {
          context.checkThread();
          if (isOpen() && !complete.get()) {
            if (error != null) {
              LOGGER.warn("{} - {}", context.getMember().id(), error.getMessage());
              quorum.fail();
            } else {
              if (response.term() > context.getTerm()) {
                context.setTerm(response.term());
              }
              if (!response.accepted()) {
                LOGGER.debug("{} - Received rejected poll from {}", context.getMember().id(), member);
                quorum.fail();
              } else if (response.term() != context.getTerm()) {
                LOGGER.debug("{} - Received accepted poll for a different term from {}", context.getMember().id(), member);
                quorum.fail();
              } else {
                LOGGER.debug("{} - Received accepted poll from {}", context.getMember().id(), member);
                quorum.succeed();
              }
            }
          }
        }, context.getContext());
      });
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
    if (heartbeatTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getMember().id());
      heartbeatTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelHeartbeatTimeout);
  }

}
