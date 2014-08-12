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
package net.kuujo.copycat;

import java.util.logging.Logger;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;

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
class Follower extends BaseState {
  private static final Logger logger = Logger.getLogger(Follower.class.getCanonicalName());
  private long currentTimer;
  private boolean shutdown = true;

  @Override
  public synchronized void init(CopyCatContext context) {
    shutdown = false;
    super.init(context);
    context.setCurrentLeader(null);
    resetTimer();
  }

  /**
   * Resets the election timer.
   */
  private synchronized void resetTimer() {
    if (!shutdown) {
      // If a timer is already set, cancel the timer.
      context.cancelTimer(currentTimer);

      // Reset the last voted for candidate.
      context.setLastVotedFor(null);

      // Set the election timeout in a semi-random fashion with the random range
      // being somewhere between .75 * election timeout and 1.25 * election
      // timeout.
      long delay = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4)
          + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
      currentTimer = context.startTimer(delay, new Callback<Long>() {
        @Override
        public void call(Long id) {
          // If the node has not yet voted for anyone then transition to
          // candidate and start a new election.
          currentTimer = 0;
          if (context.getLastVotedFor() == null) {
            logger.info("Election timed out. Transitioning to candidate.");
            context.transition(Candidate.class);
          } else {
            // If the node voted for a candidate then reset the election timer.
            resetTimer();
          }
        }
      });
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> responseCallback) {
    resetTimer();
    super.appendEntries(request, responseCallback);
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> responseCallback) {
    resetTimer();
    super.requestVote(request, responseCallback);
  }

  @Override
  public synchronized void destroy() {
    context.cancelTimer(currentTimer);
    shutdown = true;
  }

}
