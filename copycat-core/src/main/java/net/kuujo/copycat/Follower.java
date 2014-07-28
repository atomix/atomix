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

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

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
  private final Timer timeoutTimer = new Timer();
  private TimerTask timeoutTimerTask;

  @Override
  public void init(CopyCatContext context) {
    super.init(context);
    context.setCurrentLeader(null);
    resetTimer();
  }

  /**
   * Resets the election timer.
   */
  private synchronized void resetTimer() {
    if (timeoutTimerTask != null) {
      timeoutTimerTask.cancel();
      timeoutTimer.purge();
    }

    timeoutTimerTask = new TimerTask() {
      @Override
      public void run() {
        // If the node has not yet voted for anyone then transition to
        // candidate and start a new election.
        if (context.getLastVotedFor() == null) {
          logger.info("Election timed out. Transitioning to candidate.");
          context.transition(Candidate.class);
        } else {
          // If the node voted for a candidate then reset the election timer.
          resetTimer();
        }
      }
    };

    // Reset the last voted for candidate.
    context.setLastVotedFor(null);

    // Set the election timeout in a semi-random fashion with the random range
    // being somewhere between .75 * election timeout and 1.25 * election
    // timeout.
    timeoutTimer.schedule(timeoutTimerTask, context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4)
        + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2))));
  }

  @Override
  public void sync(SyncRequest request, AsyncCallback<SyncResponse> responseCallback) {
    resetTimer();
    super.sync(request, responseCallback);
  }

  @Override
  public void install(InstallRequest request, AsyncCallback<InstallResponse> responseCallback) {
    resetTimer();
    super.install(request, responseCallback);
  }

  @Override
  public void poll(PollRequest request, AsyncCallback<PollResponse> responseCallback) {
    resetTimer();
    super.poll(request, responseCallback);
  }

  @Override
  public void destroy() {
    timeoutTimer.cancel();
  }

}
