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

import java.util.HashSet;
import java.util.Set;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.util.AsyncLock;
import net.kuujo.copycat.util.Quorum;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A candidate state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Candidate extends BaseState {
  private static final Logger logger = LoggerFactory.getLogger(Candidate.class);
  private final AsyncLock lock = new AsyncLock();
  private Quorum quorum;
  private long electionTimer;

  Candidate(CopyCatContext context) {
    super(context);
  }

  @Override
  public void startUp(Handler<AsyncResult<Void>> doneHandler) {
    logger.info("Starting election");
    resetTimer();
    pollMembers();
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

  /**
   * Resets the election timer.
   */
  private void resetTimer() {
    // When the election timer is reset, increment the current term.
    context.setCurrentTerm(context.getCurrentTerm() + 1);
    long timeout = context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4) + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2)));
    electionTimer = context.vertx.setTimer(timeout, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        // When the election times out, clear the previous majority vote
        // check and restart the election.
        logger.info("Election timed out");
        if (quorum != null) {
          quorum.cancel();
          quorum = null;
        }
        resetTimer();
        pollMembers();
        logger.info("Restarted election");
      }
    });
  }

  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    if (quorum == null) {
      final Set<String> pollMembers = new HashSet<>(context.stateCluster.getMembers());
      quorum = new Quorum(context.stateCluster.getQuorumSize());
      quorum.setHandler(new Handler<Boolean>() {
        @Override
        public void handle(Boolean succeeded) {
          quorum = null;
          if (succeeded) {
            context.transition(CopyCatState.LEADER);
          } else {
            context.transition(CopyCatState.FOLLOWER);
          }
        }
      });

      // First, load the last log entry to get its term. We load the entry
      // by its index since the index is required by the protocol.
      context.log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.failed()) {
            quorum.cancel();
            quorum = null;
            context.transition(CopyCatState.FOLLOWER);
          } else {
            final long lastIndex = result.result();
            context.log.getEntry(lastIndex, new Handler<AsyncResult<Entry>>() {
              @Override
              public void handle(AsyncResult<Entry> result) {
                if (result.failed()) {
                  quorum.cancel();
                  quorum = null;
                  context.transition(CopyCatState.FOLLOWER);
                } else {
                  // Once we got the last log term, iterate through each current member
                  // of the cluster and poll each member for a vote.
                  final long lastTerm = result.result() != null ? result.result().term() : 0;
                  for (String member : pollMembers) {
                    context.client.poll(member, new PollRequest(context.getCurrentTerm(), context.stateCluster.getLocalMember(), lastIndex, lastTerm), new Handler<AsyncResult<PollResponse>>() {
                      @Override
                      public void handle(AsyncResult<PollResponse> result) {
                        // If the election is null then that means it was
                        // already finished,
                        // e.g. a majority of nodes responded.
                        if (quorum != null) {
                          if (result.failed() || !result.result().voteGranted()) {
                            quorum.fail();
                          } else {
                            quorum.succeed();
                          }
                        }
                      }
                    });
                  }
                }
              }
            });
          }
        }
      });
    }
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.reply(context.getCurrentTerm());
  }

  @Override
  public void sync(final SyncRequest request) {
    // Acquire a lock that prevents the local log from being modified
    // during the sync.
    lock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        doSync(request, new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            // If the request term is greater than the current term then this
            // indicates that another leader was already elected. Update the
            // current leader and term and transition back to a follower.
            if (request.term() > context.getCurrentTerm()) {
              context.setCurrentLeader(request.leader());
              context.setCurrentTerm(request.term());
              context.transition(CopyCatState.FOLLOWER);
            }

            // Reply to the request.
            if (result.failed()) {
              request.error(result.cause());
            } else {
              request.reply(context.getCurrentTerm(), result.result());
            }

            // Release the log lock.
            lock.release();
          }
        });
      }
    });
  }

  @Override
  public void poll(PollRequest request) {
    if (request.candidate().equals(context.cluster().getLocalMember())) {
      request.reply(context.getCurrentTerm(), true);
      context.setLastVotedFor(context.cluster().getLocalMember());
    } else {
      request.reply(context.getCurrentTerm(), false);
    }
  }

  @Override
  public void submit(SubmitRequest request) {
    request.error("Not a leader.");
  }

  @Override
  public void shutDown(Handler<AsyncResult<Void>> doneHandler) {
    if (electionTimer > 0) {
      context.vertx.cancelTimer(electionTimer);
      electionTimer = 0;
    }
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

}
