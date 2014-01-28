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

import java.util.Set;

import net.kuujo.raft.log.CommandEntry;
import net.kuujo.raft.log.ConfigurationEntry;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.Entry.Type;
import net.kuujo.raft.protocol.PingRequest;
import net.kuujo.raft.protocol.PollRequest;
import net.kuujo.raft.protocol.SubmitRequest;
import net.kuujo.raft.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A follower state.
 *
 * @author Jordan Halterman
 */
public class Follower extends State {
  private long timeoutTimer;

  private final Handler<Long> timeoutHandler = new Handler<Long>() {
    @Override
    public void handle(Long timerID) {
      if (context.votedFor() == null) {
        context.transition(StateType.CANDIDATE);
        timeoutTimer = 0;
      }
      else {
        resetTimer();
      }
    }
  };

  @Override
  public void startUp(Handler<Void> doneHandler) {
    resetTimer();
    doneHandler.handle((Void) null);
  }

  @Override
  public void configure(Set<String> members) {
    // Do nothing.
  }

  private void resetTimer() {
    if (timeoutTimer > 0) {
      vertx.cancelTimer(timeoutTimer);
    }
    timeoutTimer = vertx.setTimer(context.electionTimeout(), timeoutHandler);
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentLeader(request.leader());
      context.currentTerm(request.term());
    }
    request.reply(context.currentTerm());
    resetTimer();
  }

  @Override
  public void sync(final SyncRequest request) {
    syncRequest(request);
  }

  private void syncRequest(final SyncRequest request) {
    if (request.term() < context.currentTerm()) {
      request.reply(context.currentTerm(), false);
    }
    else {
      checkTerm(request);
    }
  }

  private void checkTerm(final SyncRequest request) {
    if (request.prevLogIndex() >= 0 && request.prevLogTerm() >= 0) {
      checkPreviousLogEntry(request);
    }
    else {
      checkEntry(request);
    }
  }

  private void checkPreviousLogEntry(final SyncRequest request) {
    log.containsEntry(request.prevLogIndex(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          request.error(result.cause());
        }
        else if (!result.result()) {
          request.reply(context.currentTerm(), false);
        }
        else {
          log.entry(request.prevLogIndex(), new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              if (result.failed()) {
                request.error(result.cause());
              }
              else if (result.result().term() != request.prevLogTerm()) {
                request.reply(context.currentTerm(), false);
              }
              else {
                checkEntry(request);
              }
            }
          });
        }
      }
    });
  }

  private void checkEntry(final SyncRequest request) {
    if (request.hasEntry()) {
      log.containsEntry(request.prevLogIndex() + 1, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            request.error(result.cause());
          }
          else if (result.result()) {
            log.entry(request.prevLogIndex() + 1, new Handler<AsyncResult<Entry>>() {
              @Override
              public void handle(AsyncResult<Entry> result) {
                if (result.failed()) {
                  request.error(result.cause());
                }
                else if (result.result().term() != request.entry().term()) {
                  log.removeAfter(request.prevLogIndex(), new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> result) {
                      if (result.failed()) {
                        request.error(result.cause());
                      }
                      else {
                        appendEntry(request);
                      }
                    }
                  });
                }
                else {
                  appendEntry(request);
                }
              }
            });
          }
          else {
            appendEntry(request);
          }
        }
      });
    }
    else {
      checkCommits(request);
    }
  }

  private void appendEntry(final SyncRequest request) {
    log.appendEntry(request.entry(), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          request.error(result.cause());
        }
        else {
          updateCommits(request);
        }
      }      
    });
  }

  private void checkCommits(final SyncRequest request) {
    if (request.commit() > context.commitIndex()) {
      updateCommits(request);
    }
    else {
      request.reply(context.currentTerm(), true);
      updateTerm(request);
    }
  }

  private void updateCommits(final SyncRequest request) {
    log.lastIndex(new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          request.error(result.cause());
        }
        else {
          final long lastIndex = result.result();
          context.commitIndex(Math.min(request.commit(), lastIndex));
          log.floor(Math.min(context.commitIndex(), context.lastApplied()), new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                request.error(result.cause());
              }
              else {
                recursiveApply(context.lastApplied(), Math.min(context.commitIndex(), lastIndex), request);
              }
            }
          });
        }
      }
    });
  }

  private void recursiveApply(final long index, final long ceiling, final SyncRequest request) {
    log.entry(index, new Handler<AsyncResult<Entry>>() {
      @Override
      public void handle(AsyncResult<Entry> result) {
        if (result.failed()) {
          request.reply(context.currentTerm(), true);
          updateTerm(request);
        }
        else {
          doApply(index, ceiling, request, result.result());
        }
      }
    });
  }

  private void doApply(final long index, final long ceiling, final SyncRequest request, final Entry entry) {
    // Since the log may be working asynchronously, make sure this is the next
    // entry to be applied to the state machine. If it isn't then recursively
    // attempt to apply the command to the state machine.
    if (context.lastApplied() == index-1) {
      if (entry.type().equals(Type.COMMAND)) {
        stateMachine.applyCommand(((CommandEntry) entry).command());
      }
      else if (entry.type().equals(Type.CONFIGURATION)) {
        context.configs().get(0).addAll(((ConfigurationEntry) entry).members());
        context.removeConfig();
      }
      context.lastApplied(index);
      log.floor(Math.min(context.commitIndex(), index), new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed() || index == ceiling) {
            request.reply(context.currentTerm(), true);
            updateTerm(request);
          }
          else  {
            recursiveApply(index+1, ceiling, request);
          }
        }
      });
    }
    else {
      vertx.runOnContext(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          doApply(index, ceiling, request, entry);
        }
      });
    }
  }

  private void updateTerm(final SyncRequest request) {
    resetTimer();
    if (request.term() > context.currentTerm()) {
      context.currentLeader(request.leader());
      context.currentTerm(request.term());
    }
  }

  @Override
  public void poll(final PollRequest request) {
    if (request.term() < context.currentTerm()) {
      request.reply(context.currentTerm(), false);
    }
    else if (context.votedFor() == null || context.votedFor().equals(request.candidate())) {
      log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.failed()) {
            request.error(result.cause());
          }
          else {
            final long lastIndex = result.result();
            log.lastTerm(new Handler<AsyncResult<Long>>() {
              @Override
              public void handle(AsyncResult<Long> result) {
                if (result.failed()) {
                  request.error(result.cause());
                }
                else {
                  final long lastTerm = result.result();
                  if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
                    request.reply(context.currentTerm(), true);
                  }
                  else {
                    request.reply(context.currentTerm(), false);
                  }
                }
              }
            });
          }
        }
      });
    }
    else {
      request.reply(context.currentTerm(), false);
    }
    resetTimer();
  }

  @Override
  public void submit(SubmitRequest request) {
    request.error("Not a leader.");
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    if (timeoutTimer > 0) {
      vertx.cancelTimer(timeoutTimer);
      timeoutTimer = 0;
    }
    doneHandler.handle((Void) null);
  }

}
