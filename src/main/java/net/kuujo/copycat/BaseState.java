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

import java.util.Iterator;
import java.util.Set;

import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Entry.Type;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * Base replica state implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseState {
  protected final CopyCatContext context;

  BaseState(CopyCatContext context) {
    this.context = context;
  }

  /**
   * Starts up the state.
   * 
   * @param doneHandler A handler to be called once the state is started up.
   */
  public abstract void startUp(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Executes a ping request.
   * 
   * @param request The request to execute.
   */
  public abstract void ping(PingRequest request);

  /**
   * Executes a sync request.
   * 
   * @param request The request to execute.
   */
  public abstract void sync(SyncRequest request);

  /**
   * Executes a poll request.
   * 
   * @param request The request to execute.
   */
  public abstract void poll(PollRequest request);

  /**
   * Executes a submit command request.
   * 
   * @param request The request to execute.
   */
  public abstract void submit(SubmitRequest request);

  /**
   * Tears down the state.
   * 
   * @param doneHandler A handler to be called once the state is shut down.
   */
  public abstract void shutDown(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Handles a sync request.
   */
  protected void doSync(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getCurrentTerm()) {
      new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
    } else {
      // Otherwise, continue on to check the log consistency.
      checkConsistency(request, doneHandler);
    }
  }

  /**
   * Checks log consistency for a sync request.
   */
  private void checkConsistency(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // If a previous log index and term were provided then check to ensure
    // that they match this node's previous log index and term.
    if (request.prevLogIndex() > 0 && request.prevLogTerm() > 0) {
      checkPreviousEntry(request, doneHandler);
    } else {
      // Otherwise, continue on to check the entry being appended.
      appendEntries(request, doneHandler);
    }
  }

  /**
   * Checks that the given previous log entry of a sync request matches the
   * previous log entry of this node.
   */
  private void checkPreviousEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // Check whether the log contains an entry at prevLogIndex.
    context.log.containsEntry(request.prevLogIndex(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        // If we failed to check the log for the previous entry then fail the request.
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        } else if (!result.result()) {
          // If no log entry was found at the previous log index then return false.
          // This will cause the leader to decrement this node's nextIndex and
          // ultimately retry with the leader's previous log entry.
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
        } else {
          // If the log entry exists then load the entry.
          context.log.getEntry(request.prevLogIndex(), new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              // If the last log entry's term is not the same as the given
              // prevLogTerm then return false. This will cause the leader to
              // decrement this node's nextIndex and ultimately retry with the
              // leader's previous log entry so that the inconsistent entry
              // can be overwritten.
              if (result.failed()) {
                new DefaultFutureResult<Boolean>(result.cause()).setHandler(doneHandler);
              } else if (result.result().term() != request.prevLogTerm()) {
                new DefaultFutureResult<Boolean>(false).setHandler(doneHandler);
              } else {
                // Finally, if the log appears to be consistent then continue on
                // to remove invalid entries from the log.
                appendEntries(request, doneHandler);
              }
            }
          });
        }
      }
    });
  }

  /**
   * Appends request entries to the log.
   */
  private void appendEntries(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    appendEntries(request.prevLogIndex(), request.entries().iterator(), request, doneHandler);
  }

  /**
   * Appends request entries to the log.
   */
  private void appendEntries(final long prevIndex, final Iterator<Entry> iterator, final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    if (iterator.hasNext()) {
      final long index = prevIndex+1;
      final Entry entry = iterator.next();
      // Load the entry from the local log.
      context.log.getEntry(index, new Handler<AsyncResult<Entry>>() {
        @Override
        public void handle(AsyncResult<Entry> result) {
          // If we failed to load the entry then fail the request. It should
          // be retried.
          if (result.failed()) {
            new DefaultFutureResult<Boolean>(result.cause()).setHandler(doneHandler);
          } else if (result.result() == null) {
            // If the log does not contain an entry at this index then this
            // indicates no conflict, append the new entry.
            context.log.appendEntry(entry, new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                // If we failed to append the new entry to the log then fail the request.
                // Once the entry has been appended no more failures occur.
                if (result.failed()) {
                  new DefaultFutureResult<Boolean>(result.cause()).setHandler(doneHandler);
                } else {
                  // Once the new entry has been appended, continue on to
                  // append the next entry.
                  appendEntries(index, iterator, request, doneHandler);
                }
              }
            });
          }
          // If the local log's equivalent entry's term does not match the
          // synced entry's term then that indicates that it came from a
          // different leader. The log must be purged of this entry and all
          // entries following it.
          else if (result.result().term() != entry.term()) {
            // Remove all entries after the previous log index.
            context.log.removeAfter(index-1, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Boolean>(result.cause()).setHandler(doneHandler);
                } else {
                  // Finally, append the entry to the local log.
                  context.log.appendEntry(entry, new Handler<AsyncResult<Long>>() {
                    @Override
                    public void handle(AsyncResult<Long> result) {
                      // If we failed to append the new entry to the log then fail the
                      // request.
                      // Once the entry has been appended no more failures occur.
                      if (result.failed()) {
                        new DefaultFutureResult<Boolean>(result.cause()).setHandler(doneHandler);
                      } else {
                        // Once the new entry has been appended, continue on to
                        // append the next entry.
                        appendEntries(index, iterator, request, doneHandler);
                      }
                    }
                  });
                }
              }
            });
          } else {
            // If the local log's equivalent entry's term matched the synced
            // entry's term then we know that the data also matched, so we don't
            // need to do any cleaning of the logs. Simply continue on to check
            // the next entry.
            appendEntries(index, iterator, request, doneHandler);
          }
        }
      });
    } else {
      checkApplyCommits(request, doneHandler);
    }
  }

  /**
   * Checks for entries that have been committed and applies committed entries
   * to the local state machine.
   */
  private void checkApplyCommits(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply
    // them.
    if (request.commit() > context.getCommitIndex() || context.getCommitIndex() > context.getLastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      context.log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.succeeded()) {
            final long lastIndex = result.result();
            context.setCommitIndex(Math.min(request.commit(), lastIndex));

            // If the updated commit index indicates that commits remain to be
            // applied to the state machine, iterate entries and apply them.
            if (context.getCommitIndex() > Math.min(context.getLastApplied(), lastIndex)) {
              recursiveApplyCommits(context.getLastApplied() + 1, Math.min(context.getCommitIndex(), lastIndex), request, doneHandler);
            } else {
              new DefaultFutureResult<Boolean>(true).setHandler(doneHandler);
            }
          } else {
            new DefaultFutureResult<Boolean>(true).setHandler(doneHandler);
          }
        }
      });
    } else {
      // Otherwise, check whether the current term needs to be updated and reply
      // true to the sync request.
      new DefaultFutureResult<Boolean>(true).setHandler(doneHandler);
    }
  }

  /**
   * Iteratively applies commits to the local state machine.
   */
  private void recursiveApplyCommits(final long index, final long ceiling, final SyncRequest request,
      final Handler<AsyncResult<Boolean>> doneHandler) {
    if (index <= ceiling) {
      // Load the log entry to be committed to the state machine.
      context.log.getEntry(index, new Handler<AsyncResult<Entry>>() {
        @Override
        public void handle(AsyncResult<Entry> result) {
          // If loading an entry fails, simply return true to the sync request.
          // We don't want to continue on to apply entries out of order.
          // The local state will maintain that the entry was never applied to
          // the state machine, and eventually it will attempt to apply the entry
          // on the next sync request.
          if (result.failed()) {
            new DefaultFutureResult<Boolean>(true).setHandler(doneHandler);
          } else {
            // If the entry was successfully loaded, apply it to the state machine.
            Entry entry = result.result();
            if (entry == null) {
              new DefaultFutureResult<Boolean>(true).setHandler(doneHandler);
            } else {
              // If the entry type is a command, apply the entry to the state machine.
              if (entry.type().equals(Type.COMMAND)) {
                CommandEntry command = (CommandEntry) entry;
                try {
                  context.stateMachine.applyCommand(command.command(), command.args());
                } catch (Exception e) {
                }
              }
              // If this is a configuration entry, update cluster membership. Since the
              // configuration was replicated to this node, it contains the *combined*
              // cluster membership during two-phase cluster configuration changes, so it's
              // safe to simply override the current cluster configuration.
              else if (entry.type().equals(Type.CONFIGURATION)) {
                Set<String> members = ((ConfigurationEntry) entry).members();
                members.remove(context.stateCluster.getLocalMember());
                context.stateCluster.setRemoteMembers(members);
              }
  
              // Continue on to apply the next commit.
              recursiveApplyCommits(index+1, ceiling, request, doneHandler);
            }
          }
        }
      });
    } else {
      new DefaultFutureResult<Boolean>(true).setHandler(doneHandler);
    }
  }

  /**
   * Handles a poll request.
   */
  protected void doPoll(final PollRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    // This future will be set to the ultimate yes/no (true/false) vote for the given request.
    final Future<Boolean> future = new DefaultFutureResult<Boolean>().setHandler(doneHandler);

    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
    }

    // If the requesting candidate is the current node then vote for self.
    if (request.candidate().equals(context.cluster().getLocalMember())) {
      future.setResult(true);
      context.setLastVotedFor(request.candidate());

    // If the requesting candidate is not a known member of the cluster (to
    // this replica) then reject the vote. This helps ensure that new cluster
    // members cannot become leader until at least a majority of the cluster
    // has been notified of their membership.
    } else if (!context.stateCluster.getMembers().contains(request.candidate())) {
      future.setResult(false);
    } else {
      // If the request term is less than the current term then don't
      // vote for the candidate.
      if (request.term() < context.getCurrentTerm()) {
        future.setResult(false);
      }
      // If we haven't yet voted or already voted for this candidate then check
      // that the candidate's log is at least as up-to-date as the local log.
      else if (context.getLastVotedFor() == null || context.getLastVotedFor().equals(request.candidate())) {
        // It's possible that the last log index could be 0, indicating that
        // the log does not contain any entries. If that is the cases then
        // the log must *always* be at least as up-to-date as all other
        // logs.
        context.log.lastIndex(new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.failed()) {
              future.setFailure(result.cause());
            } else {
              final long lastIndex = result.result();
              if (lastIndex == 0) {
                future.setResult(true);
                context.setLastVotedFor(request.candidate());
              } else {
                // Load the log entry to get the term. We load the log entry rather
                // than the log term to ensure that we're receiving the term from
                // the same entry as the loaded last log index.
                context.log.getEntry(lastIndex, new Handler<AsyncResult<Entry>>() {
                  @Override
                  public void handle(AsyncResult<Entry> result) {
                    // If the entry loading failed then don't vote for the candidate.
                    // If the log entry was null then don't vote for the candidate.
                    // This may simply result in no clear winner in the election, but
                    // it's better than an imperfect leader being elected due to a
                    // brief failure of the event bus.
                    if (result.failed() || result.result() == null) {
                      future.setResult(false);
                    } else {
                      final long lastTerm = result.result().term();
                      if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
                        future.setResult(true);
                        context.setLastVotedFor(request.candidate());
                      } else {
                        future.setResult(false);
                        context.setLastVotedFor(null); // Reset voted for.
                      }
                    }
                  }
                });
              }
            }
          }
        });
      } else {
        // If we've already voted for someone else then don't vote for the
        // candidate.
        future.setResult(false);
      }
    }
  }

}
