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

import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.util.Quorum;

/**
 * Candidate state.<p>
 *
 * The candidate state is a brief state in which the replica is
 * a candidate in an election. During its candidacy, the replica
 * will send vote requests to all other nodes in the cluster. Based
 * on the vote result, the replica will then either transition back
 * to <code>Follower</code> or become the leader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Candidate extends RaftState {
  private static final Logger logger = Logger.getLogger(Candidate.class.getCanonicalName());
  private Quorum quorum;
  private ScheduledFuture<Void> currentTimer;

  @Override
  public void init(RaftStateContext context) {
    super.init(context);
    logger.info(String.format("%s starting election", state.context().cluster().config().getLocalMember()));
    resetTimer();
  }

  /**
   * Resets the election timer.
   */
  private synchronized void resetTimer() {
    // Cancel the current timer task and purge the election timer of cancelled tasks.
    if (currentTimer != null) {
      currentTimer.cancel(true);
    }

    // When the election timer is reset, increment the current term and
    // restart the election.
    state.setCurrentTerm(state.getCurrentTerm() + 1);
    long delay = state.context().config().getElectionTimeout() - (state.context().config().getElectionTimeout() / 4) + (Math.round(Math.random() * (state.context().config().getElectionTimeout() / 2)));
    currentTimer = state.context().config().getTimerStrategy().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      logger.info(String.format("%s election timed out", state.context().cluster().config().getLocalMember()));
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      resetTimer();
      logger.info(String.format("%s restarted election", state.context().cluster().config().getLocalMember()));
    }, delay, TimeUnit.MILLISECONDS);

    state.election().start().whenComplete((elected, error) -> {
      if (error == null) {
        if (elected) {
          state.transition(Leader.class);
        } else {
          state.transition(Follower.class);
        }
      } else {
        state.transition(Follower.class);
      }
    });
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > state.getCurrentTerm()) {
      state.setCurrentTerm(request.term());
      state.setCurrentLeader(null);
      state.setLastVotedFor(null);
      state.transition(Follower.class);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (!request.candidate().equals(state.context().cluster().config().getLocalMember())) {
      return CompletableFuture.completedFuture(new RequestVoteResponse(request.id(), state.getCurrentTerm(), false));
    } else {
      return super.requestVote(request);
    }
  }

  @Override
  public synchronized void destroy() {
    if (currentTimer != null) {
      currentTimer.cancel(true);
    }
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

}
