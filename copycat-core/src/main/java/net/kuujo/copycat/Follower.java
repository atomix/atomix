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
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;

/**
 * A follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Follower extends BaseState {
  private static final Logger logger = Logger.getLogger(Follower.class.getCanonicalName());
  private final Timer timeoutTimer = new Timer();
  private final TimerTask timeoutTimerTask = new TimerTask() {
    @Override
    public void run() {
      // If the node has not yet voted for anyone then transition to
      // candidate and start a new election.
      if (context.getLastVotedFor() == null) {
        logger.info("Election timed out. Transitioning to candidate.");
        context.transition(CopyCatState.CANDIDATE);
      } else {
        // If the node voted for a candidate then reset the election timer.
        resetTimer();
      }
    }
  };

  Follower(CopyCatContext context) {
    super(context);
  }

  @Override
  void init() {
    resetTimer();
  }

  /**
   * Resets the internal election timer.
   */
  private void resetTimer() {
    // Cancel any existing timers.
    timeoutTimer.cancel();

    // Reset the last voted for candidate.
    context.setLastVotedFor(null);

    // Set the election timeout in a semi-random fashion with the random range
    // being somewhere between .75 * election timeout and 1.25 * election
    // timeout.
    timeoutTimer.schedule(timeoutTimerTask, context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4)
        + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2))));
  }

  @Override
  void ping(PingRequest request) {
    // If the request indicates a term higher than the local current term then
    // increment the term and set the indicated leader.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
    }
    // It's possible that this node and the requesting node both started an election
    // for the same term, with the requesting node ultimately winning the election.
    // In that case, the request term will match the local term, but a leader may
    // not be known. Set the current term leader if it's not already known.
    else if (request.term() == context.getCurrentTerm() && context.getCurrentLeader() == null) {
      context.setCurrentLeader(request.leader());
    }
    request.respond(context.getCurrentTerm());
    resetTimer();
  }

  @Override
  void sync(final SyncRequest request) {
    // Reset the election timer.
    resetTimer();

    boolean result = doSync(request);

    // If the request term is greater than the current term then update
    // the current leader and term.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
    }

    // Respond to the request.
    request.respond(context.getCurrentTerm(), result);

    // Reset the election timer.
    resetTimer();
  }

  @Override
  void install(final InstallRequest request) {
    resetTimer();
    doInstall(request);

    // If the request term is greater than the current term then update
    // the current leader and term.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
    }

    // Respond to the request.
    request.respond(context.getCurrentTerm());
  }

  @Override
  void poll(final PollRequest request) {
    // Its important that these two steps be done separately since the poll
    // request handling can cause the current term to be incremented. We
    // need to make sure the term is incremented prior to building the response.
    boolean result = doPoll(request);
    request.respond(context.getCurrentTerm(), result);
    resetTimer();
  }

  @Override
  void submit(SubmitRequest request) {
    // This node should never receive a submit request. All submits should
    // be automatically forwarded to the leader.
    request.respond("Not a leader");
  }

  @Override
  void destroy() {
    timeoutTimer.cancel();
  }

}
