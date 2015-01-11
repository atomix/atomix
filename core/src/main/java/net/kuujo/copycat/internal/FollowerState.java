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
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(FollowerState.class);
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
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenRun(this::startTimer);
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startTimer() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getLocalMember());
    resetHeartbeatTimer();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimer() {
    context.checkThread();
    if (isClosed()) return;

    // If a timer is already set, cancel the timer.
    if (currentTimer != null) {
      LOGGER.debug("{} - Reset heartbeat timeout", context.getLocalMember());
      currentTimer.cancel(false);
    }

    // Reset the last voted for candidate.
    context.setLastVotedFor(null);

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
        resetHeartbeatTimer();
      }
    }, delay, TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    resetHeartbeatTimer();
    return super.ping(request);
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    resetHeartbeatTimer();
    return super.append(request);
  }

  @Override
  protected PollResponse handlePoll(PollRequest request) {
    // Reset the heartbeat timer if we voted for another candidate.
    PollResponse response = super.handlePoll(request);
    if (response.voted()) {
      resetHeartbeatTimer();
    }
    return response;
  }

  /**
   * Cancels the heartbeat timer.
   */
  private void cancelTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getLocalMember());
      currentTimer.cancel(false);
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelTimer);
  }

}
