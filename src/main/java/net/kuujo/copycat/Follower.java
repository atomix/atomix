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

import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.util.AsyncLock;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Follower extends BaseState {
  private static final Logger logger = LoggerFactory.getLogger(Follower.class);
  private long timeoutTimer;
  private final AsyncLock lock = new AsyncLock();

  Follower(CopyCatContext context) {
    super(context);
  }

  /**
   * An election timeout handler.
   *
   * If this handler is ever called, it indicates to the follower that it needs to
   * transition to a candidate and begin the voting process. This timer will be
   * periodically reset according to heartbeats from the cluster leader.
   */
  private final Handler<Long> timeoutHandler = new Handler<Long>() {
    @Override
    public void handle(Long timerID) {
      // If the node has not yet voted for anyone then transition to
      // candidate and start a new election.
      if (context.getLastVotedFor() == null) {
        logger.info("Election timed out. Transitioning to candidate.");
        context.transition(CopyCatState.CANDIDATE);
        timeoutTimer = 0;
      } else {
        // If the node voted for a candidate then reset the election timer.
        resetTimer();
      }
    }
  };

  @Override
  public void startUp(Handler<AsyncResult<Void>> doneHandler) {
    resetTimer();
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

  /**
   * Resets the internal election timer.
   */
  private void resetTimer() {
    if (timeoutTimer > 0) {
      context.vertx.cancelTimer(timeoutTimer);
    }
    // Reset the last voted for candidate.
    context.setLastVotedFor(null);

    // Set the election timeout in a semi-random fashion with the random range
    // being somewhere between .75 * election timeout and 1.25 * election
    // timeout.
    timeoutTimer = context.vertx.setTimer(context.config().getElectionTimeout() - (context.config().getElectionTimeout() / 4)
        + (Math.round(Math.random() * (context.config().getElectionTimeout() / 2))), timeoutHandler);
  }

  @Override
  public void ping(PingRequest request) {
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
    request.reply(context.getCurrentTerm());
    resetTimer();
  }

  @Override
  public void sync(final SyncRequest request) {
    // Reset the election timer.
    resetTimer();

    // Acquire a lock that prevents the local log from being modified
    // during the sync.
    lock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        doSync(request, new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            // If the request term is greater than the current term then update
            // the current leader and term.
            if (request.term() > context.getCurrentTerm()) {
              context.setCurrentLeader(request.leader());
              context.setCurrentTerm(request.term());
            }

            // Reply to the request.
            if (result.failed()) {
              request.error(result.cause());
            } else {
              request.reply(context.getCurrentTerm(), result.result());
            }

            // Reset the election timer again.
            resetTimer();

            // Release the log lock.
            lock.release();
          }
        });
      }
    });
  }

  @Override
  public void poll(final PollRequest request) {
    doPoll(request, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          request.error(result.cause());
        } else {
          request.reply(context.getCurrentTerm(), result.result());
        }
      }
    });
    resetTimer();
  }

  @Override
  public void submit(SubmitRequest request) {
    // This node should never receive a submit request. All submits should
    // be automatically forwarded to the leader.
    request.error("Not a leader");
  }

  @Override
  public void shutDown(Handler<AsyncResult<Void>> doneHandler) {
    if (timeoutTimer > 0) {
      context.vertx.cancelTimer(timeoutTimer);
      timeoutTimer = 0;
    }
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

}
