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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.ProtocolClient;
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
    pollMembers();
  }

  /**
   * Polls all the members of the cluster for votes.
   */
  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    if (quorum == null) {
      final Set<Member> pollMembers = state.context().cluster().members();
      quorum = new Quorum(state.context().cluster().config().getQuorumSize(), (succeeded) -> {
        quorum = null;
        if (succeeded) {
          state.transition(Leader.class);
        } else {
          state.transition(Follower.class);
        }
      });

      // First, load the last log entry to get its term. We load the entry
      // by its index since the index is required by the protocol.
      final long lastIndex = state.context().log().lastIndex();
      Entry lastEntry = state.context().log().getEntry(lastIndex);

      // Once we got the last log term, iterate through each current member
      // of the cluster and poll each member for a vote.
      final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
      for (Member member : pollMembers) {
        final ProtocolClient client = member.protocol().client();
        client.connect().whenCompleteAsync((result1, error1) -> {
          if (error1 != null) {
            quorum.fail();
          } else {
            client.requestVote(new RequestVoteRequest(state.nextCorrelationId(), state.getCurrentTerm(), state.context().cluster().config().getLocalMember(), lastIndex, lastTerm)).whenCompleteAsync((result2, error2) -> {
              client.close();
              if (quorum != null) {
                if (error2 != null || !result2.voteGranted()) {
                  quorum.fail();
                } else {
                  quorum.succeed();
                }
              }
            });
          }
        });
      }
    }
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
