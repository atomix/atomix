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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.protocol.rpc.AppendRequest;
import net.kuujo.copycat.protocol.rpc.AppendResponse;
import net.kuujo.copycat.protocol.rpc.PollRequest;
import net.kuujo.copycat.protocol.rpc.PollResponse;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class FollowerState extends ActiveState {
  private final Random random = new Random();
  private ScheduledFuture<?> currentTimer;

  FollowerState(CopycatStateContext context) {
    super(context);
  }

  @Override
  public CopycatState state() {
    return CopycatState.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open().thenRun(this::startHeartbeatTimeout);
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimeout() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getLocalMember());
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
      LOGGER.debug("{} - Reset heartbeat timeout", context.getLocalMember());
      currentTimer.cancel(false);
    }

    // Set the election timeout in a semi-random fashion with the random range
    // being election timeout and 2 * election timeout.
    long delay = context.getElectionTimeout() + (random.nextInt((int) context.getElectionTimeout()) % context.getElectionTimeout());
    currentTimer = context.executor().schedule(() -> {
      // If the node has not yet voted for anyone then transition to
      // candidate and start a new election.
      currentTimer = null;
      if (context.getLastVotedFor() == null) {
        LOGGER.info("{} - Heartbeat timed out in {} milliseconds", context.getLocalMember(), delay);
        transition(CopycatState.CANDIDATE);
      } else {
        // If the node voted for a candidate then reset the election timer.
        resetHeartbeatTimeout();
      }
    }, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    resetHeartbeatTimeout();
    CompletableFuture<AppendResponse> response = super.append(request);
    resetHeartbeatTimeout();
    return response;
  }

  @Override
  protected PollResponse handlePoll(PollRequest request) {
    // Reset the heartbeat timeout if we voted for another candidate.
    PollResponse response = super.handlePoll(request);
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
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getLocalMember());
      currentTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelHeartbeatTimeout);
  }

}
