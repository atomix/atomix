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

import java.util.Set;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
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
class Candidate extends BaseState {
  private static final Logger logger = Logger.getLogger(Candidate.class.getCanonicalName());
  private Quorum quorum;
  private long currentTimer;

  @Override
  public void init(CopyCatContext context) {
    super.init(context);
    logger.info(String.format("%s starting election", context.cluster.config().getLocalMember()));
    resetTimer();
  }

  /**
   * Resets the election timer.
   */
  private synchronized void resetTimer() {
    // Cancel the current timer task and purge the election timer of cancelled tasks.
    context.cancelTimer(currentTimer);

    // When the election timer is reset, increment the current term and
    // restart the election.
    context.setCurrentTerm(context.getCurrentTerm() + 1);
    long delay = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4) + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
    currentTimer = context.startTimer(delay, new Callback<Long>() {
      @Override
      public void call(Long id) {
        // When the election times out, clear the previous majority vote
        // check and restart the election.
        logger.info(String.format("%s election timed out", context.cluster.config().getLocalMember()));
        if (quorum != null) {
          quorum.cancel();
          quorum = null;
        }
        resetTimer();
        logger.info(String.format("%s restarted election", context.cluster.config().getLocalMember()));
      }
    });
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
      final Set<Member> pollMembers = context.cluster.members();
      quorum = new Quorum(context.cluster.config().getQuorumSize());
      quorum.setCallback(new Callback<Boolean>() {
        @Override
        public void call(Boolean succeeded) {
          quorum = null;
          if (succeeded) {
            context.transition(Leader.class);
          } else {
            context.transition(Follower.class);
          }
        }
      });

      // First, load the last log entry to get its term. We load the entry
      // by its index since the index is required by the protocol.
      final long lastIndex = context.log.lastIndex();
      Entry lastEntry = context.log.getEntry(lastIndex);

      // Once we got the last log term, iterate through each current member
      // of the cluster and poll each member for a vote.
      final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
      for (Member member : pollMembers) {
        final ProtocolClient client = member.protocol().client();
        client.connect(new AsyncCallback<Void>() {
          @Override
          public void call(AsyncResult<Void> result) {
            if (result.succeeded()) {
              client.requestVote(new RequestVoteRequest(context.nextCorrelationId(), context.getCurrentTerm(), context.cluster.config().getLocalMember(), lastIndex, lastTerm), new AsyncCallback<RequestVoteResponse>() {
                @Override
                public void call(AsyncResult<RequestVoteResponse> result) {
                  client.close();
                  if (result.succeeded()) {
                    if (quorum != null) {
                      if (!result.value().voteGranted()) {
                        quorum.fail();
                      } else {
                        quorum.succeed();
                      }
                    }
                  } else if (quorum != null) {
                    quorum.fail();
                  }
                }
              });
            } else if (quorum != null) {
              quorum.fail();
            }
          }
        });
      }
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> responseCallback) {
    super.appendEntries(request, responseCallback);
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> responseCallback) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(null);
      context.transition(Follower.class);
    }
    // If the vote request is not for this candidate then reject the vote.
    else if (!request.candidate().equals(context.cluster.config().getLocalMember())) {
      responseCallback.call(new AsyncResult<RequestVoteResponse>(new RequestVoteResponse(request.id(), context.getCurrentTerm(), false)));
    } else {
      super.requestVote(request, responseCallback);
    }
  }

  @Override
  public synchronized void destroy() {
    context.cancelTimer(currentTimer);
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

}
