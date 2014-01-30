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
package net.kuujo.raft.state;

import net.kuujo.raft.ReplicationServiceEndpoint;
import net.kuujo.raft.StateMachine;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.Log;
import net.kuujo.raft.protocol.PollRequest;
import net.kuujo.raft.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * An abstract state implementation.
 *
 * @author Jordan Halterman
 */
public abstract class BaseState implements State {
  protected Vertx vertx;
  protected ReplicationServiceEndpoint endpoint;
  protected StateMachine stateMachine;
  protected Log log;
  protected StateContext context;

  /**
   * Sets the vertx instance.
   * 
   * @param vertx A vertx instance.
   * @return The state instance.
   */
  public State setVertx(Vertx vertx) {
    this.vertx = vertx;
    return this;
  }

  /**
   * Sets the endpoint.
   * 
   * @param endpoint An endpoint instance.
   * @return The state instance.
   */
  public State setEndpoint(ReplicationServiceEndpoint endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  /**
   * Sets the state machine.
   * 
   * @param stateMachine The state machine.
   * @return The state instance.
   */
  public State setStateMachine(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
    return this;
  }

  /**
   * Sets the log.
   * 
   * @param log A log instance.
   * @return The state instance.
   */
  public State setLog(Log log) {
    this.log = log;
    return this;
  }

  /**
   * Sets the state context.
   * 
   * @param context A state context.
   * @return The state instance.
   */
  public State setContext(StateContext context) {
    this.context = context;
    return this;
  }

  /**
   * Handles a sync request.
   */
  protected void doSync(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.currentTerm()) {
      new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
    }
    // Otherwise, continue on to check the log consistency.
    else {
      checkConsistency(request, doneHandler);
    }
  }

  /**
   * Checks log consistency for a sync request.
   */
  private void checkConsistency(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // If a previous log index and term were provided then check to ensure
    // that they match this node's previous log index and term.
    if (request.prevLogIndex() >= 0 && request.prevLogTerm() >= 0) {
      checkPreviousEntry(request, doneHandler);
    }
    // Otherwise, continue on to check the entry being appended.
    else {
      checkEntry(request, doneHandler);
    }
  }

  /**
   * Checks that the given previous log entry of a sync request matches
   * the previous log entry of this node.
   */
  private void checkPreviousEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // Check whether the log contains an entry at prevLogIndex.
    log.containsEntry(request.prevLogIndex(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        // If we failed to check the log for the previous entry then fail the request.
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        }
        // If no log entry was found at the previous log index then return false.
        // This will cause the leader to decrement this node's nextIndex and
        // ultimately retry with the leader's previous log entry.
        else if (!result.result()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
        }
        // If the log entry exists then load the entry.
        else {
          log.entry(request.prevLogIndex(), new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              if (result.failed()) {
                new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
              }
              // If the last log entry's term is not the same as the given
              // prevLogTerm then return false. This will cause the leader to
              // decrement this node's nextIndex and ultimately retry with the
              // leader's previous log entry so that the incosistent entry
              // can be overwritten.
              else if (result.result().term() != request.prevLogTerm()) {
                new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
              }
              // Finally, if the log appears to be consistent then continue on
              // to remove invalid entries from the log.
              else {
                checkEntry(request, doneHandler);
              }
            }
          });
        }
      }
    });
  }

  /**
   * Checks that a synced entry is consistent with the local log
   * and, if not, cleans the local log of invalid entries.
   */
  private void checkEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // Check if the log contains an entry at the synced entry index.
    log.containsEntry(request.prevLogIndex() + 1, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        // If we failed to check the log for the entry then fail the request.
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        }
        // If the log contains an entry at the given index then check
        // that the entry is consistent with the synced entry.
        else if (result.result()) {
          // Load the entry from the local log.
          log.entry(request.prevLogIndex() + 1, new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              // If we failed to load the entry then fail the request. It should be retried.
              if (result.failed()) {
                new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
              }
              // If the local log's equivalent entry's term does not match the
              // synced entry's term then that indicates that it came from a
              // different leader. The log must be purged of this entry and all
              // entries following it.
              else if (result.result().term() != request.entry().term()) {
                // Remove all entries after the previous log index.
                log.removeAfter(request.prevLogIndex(), new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    if (result.failed()) {
                      new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
                    }
                    // Finally, append the entry to the local log.
                    else {
                      appendEntry(request, doneHandler);
                    }
                  }
                });
              }
              // If the local log's equivalent entry's term matched the synced
              // entry's term then we know that the data also matched, so we don't
              // need to do any cleaning of the logs. Simply continue on to check
              // whether commits need to be applied to the state machine.
              else {
                checkApplyCommits(request, doneHandler);
              }
            }
          });
        }
        // If the local log did not contain any entry at the synced index then
        // simply continue on to append the new entry to the log.
        else {
          appendEntry(request, doneHandler);
        }
      }
    });
  }

  /**
   * Appends an entry to the local log.
   */
  private void appendEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // Append the synced entry to the log.
    log.appendEntry(request.entry(), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        // If we failed to append the new entry to the log then fail the request.
        // Once the entry has been appended no more failures occur.
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        }
        // Once the new entry has been appended, apply commands to the state
        // machine as necessary.
        else {
          checkApplyCommits(request, doneHandler);
        }
      }      
    });
  }

  /**
   * Checks for entries that have been committed and applies committed
   * entries to the local state machine.
   */
  private void checkApplyCommits(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (request.commit() > context.commitIndex() || context.commitIndex() > context.lastApplied()) {
      // Get the last log index.
      log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          // If for some reason we failed to load the last log index, simply fail
          // gracefully by replying true to the sync request. Next time the leader
          // syncs with this node, the commit application will be re-attempted.
          if (result.failed()) {
            new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
          }
          else {
            // Update the local commit index with min(request commit, last log index)
            final long lastIndex = result.result();
            context.commitIndex(Math.min(request.commit(), lastIndex));

            // If the updated commit index indicates that commits remain to be
            // applied to the state machine, iterate entries and apply them.
            if (context.commitIndex() > context.lastApplied()) {
              // Set the log floor. This indicates the minimum log entry that is
              // required to remain persisted. Log entries that have not yet been
              // applied to the state machine cannot be removed from the log.
              log.floor(Math.min(context.commitIndex(), context.lastApplied()), new Handler<AsyncResult<Void>>() {
                @Override
                public void handle(AsyncResult<Void> result) {
                  // If setting the log floor failed for some reason, ignore the
                  // failure. Since the log floor always increases, we simply allow
                  // more log entries to remain by not setting the log floor.
                  // iterate over committed log entries and apply them to the state machine.
                  // We apply entries up to min(local commit index, last log index)
                  if (result.succeeded()) {
                    recursiveApplyCommits(context.lastApplied()+1, Math.min(context.commitIndex(), lastIndex), request, doneHandler);
                  }
                }
              });
            }
          }
        }
      });
    }
    // Otherwise, check whether the current term needs to be updated and reply
    // true to the sync request.
    else {
      new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
    }
  }

  /**
   * Iteratively applies commits to the local state machine.
   */
  private void recursiveApplyCommits(final long index, final long ceiling, final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // Load the log entry to be committed to the state machine.
    log.entry(index, new Handler<AsyncResult<Entry>>() {
      @Override
      public void handle(AsyncResult<Entry> result) {
        // If loading an entry fails, simply return true to the sync request.
        // We don't want to continue on to apply entries out of order.
        // The local state will maintain that the entry was never applied to
        // the state machine, and eventually it will attempt to apply the entry
        // on the next sync request.
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
        }
        // If the entry was successfully loaded, apply it to the state machine.
        else {
          doApply(index, ceiling, request, result.result(), doneHandler);
        }
      }
    });
  }

  /**
   * Applies a single commit to the local state machine.
   */
  private void doApply(final long index, final long ceiling, final SyncRequest request, final Entry entry, final Handler<AsyncResult<Boolean>> doneHandler) {
    // Apply the entry to the state machine.
    context.applyEntry(entry);

    // Update the last applied index to the current index.
    context.lastApplied(index);

    // Update the log floor to the last committed index (this index).
    log.floor(index, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        // If we've finished applying all the commits, finish the sync successfully.
        if (index == ceiling) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
        }
        // Otherwise, continue on to apply the next commit. Again, we ignore
        // whether the floor application failed because it should have no
        // negative impact on the consistency of the log.
        else {
          recursiveApplyCommits(index+1, ceiling, request, doneHandler);
        }
      }
    });
  }

  /**
   * Handles a poll request.
   */
  protected void doPoll(final PollRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    final Future<Boolean> future = new DefaultFutureResult<Boolean>().setHandler(doneHandler);

    // If the request term is greater than the current term then update
    // the local current term. This will also cause the candidate voted
    // for to be reset for the new term.
    if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
    }

    // If the request term is less than the current term then don't
    // vote for the candidate.
    if (request.term() < context.currentTerm()) {
      future.setResult(false);
    }
    // If we haven't yet voted or already voted for this candidate then check
    // that the candidate's log is at least as up-to-date as the local log.
    else if (context.votedFor() == null || context.votedFor().equals(request.candidate())) {
      // Get the last log entry index.
      log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.failed()) {
            future.setFailure(result.cause());
          }
          else {
            final long lastIndex = result.result();
            // It's possible that the last log index could be -1, indicating that
            // the log does not contain any entries. If that is the cases then
            // the log must *always* be at least as up-to-date as all other logs.
            if (lastIndex < 0) {
              future.setResult(true);
              context.votedFor(request.candidate());
            }
            else {
              // Load the log entry to get the term. We load the log entry rather
              // than the log term to ensure that we're receiving the term from
              // the same entry as the loaded last log index.
              log.entry(lastIndex, new Handler<AsyncResult<Entry>>() {
                @Override
                public void handle(AsyncResult<Entry> result) {
                  // If the entry loading failed then don't vote for the candidate.
                  // If the log entry was null then don't vote for the candidate.
                  // This may simply result in no clear winner in the election, but
                  // it's better than an imperfect leader being elected due to a
                  // brief failure of the event bus.
                  if (result.failed() || result.result() == null) {
                    future.setResult(false);
                  }
                  else {
                    final long lastTerm = result.result().term();
                    if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
                      future.setResult(true);
                      context.votedFor(request.candidate());
                    }
                    else {
                      future.setResult(false);
                      context.votedFor(null); // Reset voted for.
                    }
                  }
                }
              });
            }
          }
        }
      });
    }
    // If we've already voted for someone else then don't vote for the candidate.
    else {
      future.setResult(false);
    }
  }

}
