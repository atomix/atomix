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
package net.kuujo.copycat.internal.state;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.internal.cluster.RemoteNode;
import net.kuujo.copycat.internal.log.CopycatEntry;
import net.kuujo.copycat.internal.util.Quorum;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.spi.protocol.ProtocolClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class CandidateController extends StateController {
  private static final Logger LOGGER = LoggerFactory.getLogger(CandidateController.class);
  private Quorum quorum;
  private ScheduledFuture<Void> currentTimer;

  @Override
  CopycatState state() {
    return CopycatState.CANDIDATE;
  }

  @Override
  Logger logger() {
    return LOGGER;
  }

  @Override
  void init(StateContext context) {
    super.init(context);
    LOGGER.info("{} - Starting election", context.clusterManager().localNode());
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
    context.currentTerm(context.currentTerm() + 1);
    long delay = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4) + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
    currentTimer = context.config().getTimerStrategy().schedule(() -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.info("{} - Election timed out", context.clusterManager().localNode());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      resetTimer();
      LOGGER.info("{} - Restarted election", context.clusterManager().localNode());
    }, delay, TimeUnit.MILLISECONDS);

    final AtomicBoolean complete = new AtomicBoolean();

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum((int) Math.floor(context.clusterManager().nodes().size() / 2) + 1, (elected) -> {
      complete.set(true);
      if (elected) {
        context.transition(LeaderController.class);
      } else {
        context.transition(FollowerController.class);
      }
    }).countSelf();

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    final long lastIndex = context.log().lastIndex();
    CopycatEntry lastEntry = context.log().getEntry(lastIndex);

    // Once we got the last log term, iterate through each current member
    // of the cluster and poll each member for a vote.
    LOGGER.info("{} - Polling members {}", context.clusterManager().localNode(), context.clusterManager().cluster().remoteMembers());
    final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
    for (RemoteNode node : context.clusterManager().remoteNodes()) {
      final ProtocolClient client = node.client();
      client.connect().whenComplete((result1, error1) -> {
        if (error1 != null) {
          quorum.fail();
        } else {
          LOGGER.debug("{} - Polling {}", context.clusterManager().localNode(), node.member());
          client.poll(new PollRequest(context.nextCorrelationId(), context.currentTerm(), context.clusterManager()
            .localNode()
            .member()
            .id(), lastIndex, lastTerm)).whenComplete((result2, error2) -> {
            client.close();
            if (!complete.get()) {
              if (error2 != null) {
                LOGGER.warn(context.clusterManager().localNode().toString(), error2);
                quorum.fail();
              } else if (!result2.voteGranted()) {
                LOGGER.info("{} - Received rejected vote from {}", context.clusterManager().localNode(), node.member());
                quorum.fail();
              } else if (result2.term() != context.currentTerm()) {
                LOGGER.info("{} - Received successful vote for a different term from {}", context.clusterManager()
                  .localNode(), node.member());
                quorum.fail();
              } else {
                LOGGER.info("{} - Received successful vote from {}", context.clusterManager()
                  .localNode(), node.member());
                quorum.succeed();
              }
            }
          });
        }
      });
    }
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() >= context.currentTerm()) {
      context.currentTerm(request.term());
      context.currentLeader(null);
      context.lastVotedFor(null);
      context.transition(FollowerController.class);
    }
    return super.ping(request);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
      context.currentLeader(null);
      context.lastVotedFor(null);
      context.transition(FollowerController.class);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (!request.candidate().equals(context.clusterManager().localNode().member().id())) {
      return CompletableFuture.completedFuture(logResponse(new PollResponse(logRequest(request).id(), context.currentTerm(), false)));
    } else {
      return super.poll(request);
    }
  }

  @Override
  synchronized void destroy() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling election", context.clusterManager().localNode());
      currentTimer.cancel(true);
    }
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

  @Override
  public String toString() {
    return String.format("CandidateController[context=%s]", context);
  }

}
