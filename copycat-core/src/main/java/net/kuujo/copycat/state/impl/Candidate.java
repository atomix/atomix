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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import net.kuujo.copycat.log.impl.RaftEntry;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.state.State;
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
public class Candidate extends CopycatState {
  private static final Logger logger = Logger.getLogger(Candidate.class.getCanonicalName());
  private Quorum quorum;
  private ScheduledFuture<Void> currentTimer;

  @Override
  public State.Type type() {
    return State.Type.CANDIDATE;
  }

  @Override
  public void init(CopycatStateContext context) {
    super.init(context);
    logger.info(String.format("%s starting election", context.cluster().config().getLocalMember()));
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
    context.setCurrentTerm(context.getCurrentTerm() + 1);
    long delay = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4) + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
    currentTimer = context.config().getTimerStrategy().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      logger.info(String.format("%s election timed out", context.cluster().config().getLocalMember()));
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      resetTimer();
      logger.info(String.format("%s restarted election", context.cluster().config().getLocalMember()));
    }, delay, TimeUnit.MILLISECONDS);

    final AtomicBoolean complete = new AtomicBoolean();

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum(context.cluster().config().getQuorumSize(), (elected) -> {
      complete.set(true);
      if (elected) {
        context.transition(Leader.class);
      } else {
        context.transition(Follower.class);
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    final long lastIndex = context.log().lastIndex();
    RaftEntry lastEntry = context.log().getEntry(lastIndex);

    // Once we got the last log term, iterate through each current member
    // of the cluster and poll each member for a vote.
    final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
    for (Member member : context.cluster().members()) {
      if (member.equals(context.cluster().localMember())) {
        quorum.succeed();
      } else {
        final ProtocolClient client = member.protocol().client();
        client.connect().whenCompleteAsync((result1, error1) -> {
          if (error1 != null) {
            quorum.fail();
          } else {
            client.poll(new PollRequest(context.nextCorrelationId(), context.getCurrentTerm(), context.cluster().config().getLocalMember(), lastIndex, lastTerm)).whenCompleteAsync((result2, error2) -> {
              client.close();
              if (!complete.get()) {
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
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(null);
      context.setLastVotedFor(null);
      context.transition(Follower.class);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (!request.candidate().equals(context.cluster().config().getLocalMember())) {
      return CompletableFuture.completedFuture(new PollResponse(request.id(), context.getCurrentTerm(), false));
    } else {
      return super.poll(request);
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
