/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.state.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.state.State;

/**
 * Follower state.<p>
 *
 * The follower state is the initial state of any replica once it
 * has been started, and for most replicas it remains the state for
 * most of their lifetime. Followers simply serve to listen for
 * synchronization requests from the cluster <code>Leader</code>
 * and dutifully maintain their logs according to those requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Follower extends RaftState {
  private static final Logger logger = Logger.getLogger(Follower.class.getCanonicalName());
  private ScheduledFuture<Void> currentTimer;
  private boolean shutdown = true;

  @Override
  public State.Type type() {
    return State.Type.FOLLOWER;
  }

  @Override
  public synchronized void init(RaftStateContext context) {
    shutdown = false;
    super.init(context);
    resetTimer();
  }

  /**
   * Resets the election timer.
   */
  private synchronized void resetTimer() {
    if (!shutdown) {
      // If a timer is already set, cancel the timer.
      if (currentTimer != null) {
        currentTimer.cancel(true);
      }

      // Reset the last voted for candidate.
      state.setLastVotedFor(null);

      // Set the election timeout in a semi-random fashion with the random range
      // being somewhere between .75 * election timeout and 1.25 * election
      // timeout.
      long delay = state.context().config().getElectionTimeout() - (state.context().config().getElectionTimeout() / 4)
          + (Math.round(Math.random() * (state.context().config().getElectionTimeout() / 2)));
      currentTimer = state.context().config().getTimerStrategy().schedule(() -> {
        // If the node has not yet voted for anyone then transition to
        // candidate and start a new election.
        currentTimer = null;
        if (state.getLastVotedFor() == null) {
          logger.info("Election timed out. Transitioning to candidate.");
          state.transition(Candidate.class);
        } else {
          // If the node voted for a candidate then reset the election timer.
          resetTimer();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    resetTimer();
    return super.appendEntries(request);
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    resetTimer();
    return super.requestVote(request);
  }

  @Override
  public synchronized void destroy() {
    if (currentTimer != null) {
      currentTimer.cancel(true);
    }
    shutdown = true;
  }

}
