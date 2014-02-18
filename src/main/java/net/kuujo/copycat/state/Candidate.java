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
package net.kuujo.copycat.state;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.state.impl.Majority;
import net.kuujo.copycat.state.impl.StateLock;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A candidate state.
 * 
 * @author Jordan Halterman
 */
class Candidate extends State {
  private static final Logger logger = LoggerFactory.getLogger(Candidate.class);
  private final StateLock lock = new StateLock();
  private Majority majority;
  private long electionTimer;

  @Override
  public void startUp(Handler<AsyncResult<Void>> doneHandler) {
    // When the candidate is created, increment the current term.
    context.currentTerm(context.currentTerm() + 1);
    logger.info("Starting election.");
    resetTimer();
    pollMembers();
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

  /**
   * Resets the election timer.
   */
  private void resetTimer() {
    electionTimer = vertx.setTimer(
        context.electionTimeout() - (context.electionTimeout() / 4) + (Math.round(Math.random() * (context.electionTimeout() / 2))),
        new Handler<Long>() {
          @Override
          public void handle(Long timerID) {
            // When the election times out, clear the previous majority vote
            // check and restart the election.
            logger.info("Election timed out.");
            if (majority != null) {
              majority.cancel();
              majority = null;
            }
            resetTimer();
            pollMembers();
            logger.info("Restarted election.");
          }
        });
  }

  private void pollMembers() {
    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    if (majority == null) {
      majority = new Majority(members);
      majority.start(new Handler<String>() {
        @Override
        public void handle(final String address) {
          // Load the last log entry to get its term. We do this rather
          // than
          // calling lastTerm() because the last entry could have changed
          // already.
          log.lastIndex(new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              if (result.succeeded()) {
                final long lastIndex = result.result();
                log.getEntry(lastIndex, new Handler<AsyncResult<Entry>>() {
                  @Override
                  public void handle(AsyncResult<Entry> result) {
                    if (result.succeeded() && result.result() != null) {
                      final long lastTerm = result.result().term();
                      stateClient.poll(address, new PollRequest(context.currentTerm(), context.address(), lastIndex, lastTerm),
                          new Handler<AsyncResult<PollResponse>>() {
                            @Override
                            public void handle(AsyncResult<PollResponse> result) {
                              // If the election is null then that means it was
                              // already finished,
                              // e.g. a majority of nodes responded.
                              if (majority != null) {
                                if (result.failed() || !result.result().voteGranted()) {
                                  majority.fail(address);
                                }
                                else {
                                  majority.succeed(address);
                                }
                              }
                            }
                          });
                    }
                  }
                });
              }
            }
          });
        }
      }, new Handler<Boolean>() {
        @Override
        public void handle(Boolean elected) {
          majority = null;
          if (elected) {
            context.transition(StateType.LEADER);
          }
          else {
            context.transition(StateType.FOLLOWER);
          }
        }
      });
    }
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentLeader(request.leader());
      context.currentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
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
            if (request.term() > context.currentTerm()) {
              context.currentLeader(request.leader());
              context.currentTerm(request.term());
              context.transition(StateType.FOLLOWER);
            }

            // Reply to the request.
            if (result.failed()) {
              request.error(result.cause());
            }
            else {
              request.reply(context.currentTerm(), result.result());
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
    if (request.candidate().equals(context.address())) {
      request.reply(context.currentTerm(), true);
      context.votedFor(context.address());
    }
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void submit(SubmitRequest request) {
    request.error("Not a leader.");
  }

  @Override
  public void shutDown(Handler<AsyncResult<Void>> doneHandler) {
    if (electionTimer > 0) {
      vertx.cancelTimer(electionTimer);
      electionTimer = 0;
    }
    if (majority != null) {
      majority.cancel();
      majority = null;
    }
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

}
