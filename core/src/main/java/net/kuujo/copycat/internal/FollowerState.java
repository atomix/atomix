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
import net.kuujo.copycat.protocol.AppendRequest;
import net.kuujo.copycat.protocol.AppendResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private ScheduledFuture<?> currentTimer;
  private boolean shutdown;

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
    return super.open().thenRunAsync(this::startTimer, context.executor());
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startTimer() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getLocalMember());
    resetTimer();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetTimer() {
    if (!shutdown) {
      // If a timer is already set, cancel the timer.
      if (currentTimer != null) {
        LOGGER.debug("{} - Reset heartbeat timeout", context.getLocalMember());
        currentTimer.cancel(true);
      }

      // Reset the last voted for candidate.
      context.setLastVotedFor(null);

      // Set the election timeout in a semi-random fashion with the random range
      // being somewhere between .75 * election timeout and 1.25 * election
      // timeout.
      long delay = context.getElectionTimeout() - (context.getElectionTimeout() / 4)
        + (Math.round(Math.random() * (context.getElectionTimeout() / 2)));
      currentTimer = context.executor().schedule(() -> {
        // If the node has not yet voted for anyone then transition to
        // candidate and start a new election.
        currentTimer = null;
        if (context.getLastVotedFor() == null) {
          LOGGER.info("{} - Heartbeat timed out", context.getLocalMember());
          transition(CopycatState.CANDIDATE);
        } else {
          // If the node voted for a candidate then reset the election timer.
          resetTimer();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    resetTimer();
    return super.ping(request);
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    resetTimer();
    return super.append(request);
  }

  /**
   * Cancels the heartbeat timer.
   */
  private void cancelTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getLocalMember());
      currentTimer.cancel(true);
    }
    shutdown = true;
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRunAsync(this::cancelTimer, context.executor());
  }

}
