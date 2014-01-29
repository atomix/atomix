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

import net.kuujo.raft.log.Entry;
import net.kuujo.raft.protocol.PingRequest;
import net.kuujo.raft.protocol.PollRequest;
import net.kuujo.raft.protocol.SubmitRequest;
import net.kuujo.raft.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * A follower state.
 *
 * @author Jordan Halterman
 */
public class Follower extends State {
  private long timeoutTimer;
  private final StateLock lock = new StateLock();

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
    resetTimer();
    lock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        syncRequest(request, new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            if (result.failed()) {
              request.error(result.cause());
            }
            else {
              request.reply(context.currentTerm(), result.result());
            }
            resetTimer();
            lock.release();
          }
        });
      }
    });
  }

  private void syncRequest(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    if (request.term() < context.currentTerm()) {
      new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
    }
    else {
      checkTerm(request, doneHandler);
    }
  }

  private void checkTerm(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    if (request.prevLogIndex() >= 0 && request.prevLogTerm() >= 0) {
      checkPreviousLogEntry(request, doneHandler);
    }
    else {
      checkEntry(request, doneHandler);
    }
  }

  private void checkPreviousLogEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    log.containsEntry(request.prevLogIndex(), new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        }
        else if (!result.result()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
        }
        else {
          log.entry(request.prevLogIndex(), new Handler<AsyncResult<Entry>>() {
            @Override
            public void handle(AsyncResult<Entry> result) {
              if (result.failed()) {
                new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
              }
              else if (result.result().term() != request.prevLogTerm()) {
                new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(false);
              }
              else {
                checkEntry(request, doneHandler);
              }
            }
          });
        }
      }
    });
  }

  private void checkEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    if (request.hasEntry()) {
      log.containsEntry(request.prevLogIndex() + 1, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
          }
          else if (result.result()) {
            log.entry(request.prevLogIndex() + 1, new Handler<AsyncResult<Entry>>() {
              @Override
              public void handle(AsyncResult<Entry> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
                }
                else if (result.result().term() != request.entry().term()) {
                  log.removeAfter(request.prevLogIndex(), new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> result) {
                      if (result.failed()) {
                        new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
                      }
                      else {
                        appendEntry(request, doneHandler);
                      }
                    }
                  });
                }
                else {
                  updateCommits(request, doneHandler);
                }
              }
            });
          }
          else {
            appendEntry(request, doneHandler);
          }
        }
      });
    }
    else {
      checkCommits(request, doneHandler);
    }
  }

  private void appendEntry(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    log.appendEntry(request.entry(), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        }
        else {
          updateCommits(request, doneHandler);
        }
      }      
    });
  }

  private void checkCommits(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    if (request.commit() > context.commitIndex()) {
      updateCommits(request, doneHandler);
    }
    else {
      new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
      updateTerm(request);
    }
  }

  private void updateCommits(final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    log.lastIndex(new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
        }
        else {
          final long lastIndex = result.result();
          context.commitIndex(Math.min(request.commit(), lastIndex));
          log.floor(Math.min(context.commitIndex(), context.lastApplied()), new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Boolean>().setHandler(doneHandler).setFailure(result.cause());
              }
              else {
                recursiveApply(context.lastApplied()+1, Math.min(context.commitIndex(), lastIndex), request, doneHandler);
              }
            }
          });
        }
      }
    });
  }

  private void recursiveApply(final long index, final long ceiling, final SyncRequest request, final Handler<AsyncResult<Boolean>> doneHandler) {
    log.entry(index, new Handler<AsyncResult<Entry>>() {
      @Override
      public void handle(AsyncResult<Entry> result) {
        if (result.failed()) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
          updateTerm(request);
        }
        else {
          doApply(index, ceiling, request, result.result(), doneHandler);
        }
      }
    });
  }

  private void doApply(final long index, final long ceiling, final SyncRequest request, final Entry entry, final Handler<AsyncResult<Boolean>> doneHandler) {
    context.applyEntry(entry);
    context.lastApplied(index);
    log.floor(Math.min(context.commitIndex(), index), new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed() || index == ceiling) {
          new DefaultFutureResult<Boolean>().setHandler(doneHandler).setResult(true);
          updateTerm(request);
        }
        else  {
          recursiveApply(index+1, ceiling, request, doneHandler);
        }
      }
    });
  }

  private void updateTerm(final SyncRequest request) {
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
      lock.acquire(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          log.lastIndex(new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              if (result.failed()) {
                request.error(result.cause());
                lock.release();
              }
              else {
                final long lastIndex = result.result();
                log.lastTerm(new Handler<AsyncResult<Long>>() {
                  @Override
                  public void handle(AsyncResult<Long> result) {
                    lock.release();
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
