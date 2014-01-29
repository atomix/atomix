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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.kuujo.raft.Command;
import net.kuujo.raft.log.CommandEntry;
import net.kuujo.raft.log.ConfigurationEntry;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.NoOpEntry;
import net.kuujo.raft.log.Entry.Type;
import net.kuujo.raft.protocol.PingRequest;
import net.kuujo.raft.protocol.PingResponse;
import net.kuujo.raft.protocol.PollRequest;
import net.kuujo.raft.protocol.SubmitRequest;
import net.kuujo.raft.protocol.SyncRequest;
import net.kuujo.raft.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * A leader state.
 *
 * @author Jordan Halterman
 */
public class Leader extends State {
  private final StateLock lock = new StateLock();
  private long pingTimer;
  private Set<String> members;
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap = new HashMap<>();
  private final Set<Majority> majorities = new HashSet<>();

  @Override
  public void startUp(final Handler<Void> doneHandler) {
    // Create a set of replica references in the cluster.
    members = new HashSet<>();
    replicas = new ArrayList<>();
    for (String address : context.members()) {
      if (!address.equals(context.address())) {
        members.add(address);
        Replica replica = new Replica(address);
        replicaMap.put(address, replica);
        replicas.add(replica);
      }
    }

    // Set self as the current leader.
    context.currentLeader(context.address());

    // Set up a timer for pinging cluster members.
    pingTimer = vertx.setPeriodic(context.syncInterval(), new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        for (Replica replica : replicas) {
          replica.update();
        }
      }
    });

    // Commit a NOOP entry to the log.
    lock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        log.appendEntry(new NoOpEntry(context.currentTerm()), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.succeeded()) {
              context.lastApplied(result.result());
              lock.release();

              // Immediately update all nodes.
              for (Replica replica : replicas) {
                replica.update();
              }
              doneHandler.handle((Void) null);
            }
          }
        });
      }
    });
  }

  @Override
  public void configure(final Set<String> members) {
    // Store the new configuration and add any members to the current replica sets.
    for (String address : members) {
      if (!address.equals(context.address())) {
        this.members.add(address);
        if (!replicaMap.containsKey(address)) {
          Replica replica = new Replica(address);
          replicaMap.put(address, replica);
          replicas.add(replica);
        }
      }
    }

    lock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        log.appendEntry(new ConfigurationEntry(context.currentTerm(), members), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            context.lastApplied(result.result());
          }
        });
      }
    });
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
  }

  @Override
  public void sync(final SyncRequest request) {
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
      context.transition(StateType.FOLLOWER);
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
  }

  @Override
  public void submit(final SubmitRequest request) {
    // If the state machine supports this command, apply the command to the state
    // machine based on the command type.
    if (stateMachine.hasCommand(request.command().command())) {
      Command.Type commandType = stateMachine.getCommandType(request.command().command());
      // If this is a read command then we need to contact a majority of the cluster
      // to ensure that the information is not stale. Once we've determined that
      // this node is the most up-to-date, we can simply apply the command to the
      // state machine and return the result without replicating the log.
      if (commandType.equals(Command.Type.READ)) {
        readMajority(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            request.reply(stateMachine.applyCommand(request.command()));
          }
        });
      }
      else {
        log.appendEntry(new CommandEntry(context.currentTerm(), request.command()), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.failed()) {
              request.error(result.cause());
            }
            else {
              final long index = result.result();
              writeMajority(index, new Handler<Void>() {
                @Override
                public void handle(Void arg0) {
                  JsonObject result = stateMachine.applyCommand(request.command());
                  context.lastApplied(index);
                  request.reply(result);
                }
              });
            }
          }
        });
      }
    }
    // If the state machine doesn't support the command then simply return an error.
    else {
      request.error("Command unsupported.");
    }
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    if (pingTimer > 0) {
      vertx.cancelTimer(pingTimer);
      pingTimer = 0;
    }
    // Cancel all majority vote attempts.
    for (Majority majority : majorities) {
      majority.cancel();
    }
    doneHandler.handle((Void) null);
  }

  private void updateCommitIndex(final long commitIndex) {
    // If any configuration entries are being committed, update the cluster members.
    if (context.commitIndex() < commitIndex) {
      long prevCommitIndex = context.commitIndex();
      log.entries(prevCommitIndex+1, commitIndex, new Handler<AsyncResult<List<Entry>>>() {
        @Override
        public void handle(AsyncResult<List<Entry>> result) {
          if (result.succeeded()) {
            for (Entry entry : result.result()) {
              if (entry.type().equals(Type.CONFIGURATION)) {
                // Remove the configuration from the list of configurations.
                context.removeConfig();
        
                // Recreate the members set.
                members = new HashSet<>();
                for (String address : context.members()) {
                  if (!address.equals(context.address())) {
                    members.add(address);
                  }
                }
        
                // Iterate through replicas and remove any replicas that were removed from the cluster.
                Iterator<Replica> iterator = replicas.iterator();
                while (iterator.hasNext()) {
                  Replica replica = iterator.next();
                  if (!members.contains(replica.address)) {
                    replica.updating = false;
                    iterator.remove();
                    replicaMap.remove(replica.address);
                  }
                }
              }
            }
            
            // Finally, set the new commit index. This will be sent to replicas to
            // instruct them to apply the entries to their state machines.
            context.commitIndex(commitIndex);
            log.floor(Math.min(context.commitIndex(), context.lastApplied()), new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                
              }
            });
          }
        }
      });
    }
  }

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    Collections.sort(replicas, new Comparator<Replica>() {
      @Override
      public int compare(Replica o1, Replica o2) {
        return Long.compare(o1.matchIndex, o2.matchIndex);
      }
    });

    int middle = (int) Math.ceil(replicas.size() / 2);
    updateCommitIndex(replicas.get(middle).matchIndex);
  }

  /**
   * Replicates the index to a majority of the cluster.
   */
  private void writeMajority(final long index, final Handler<Void> doneHandler) {
    final Majority majority = new Majority(members);
    majorities.add(majority);
    majority.start(new Handler<String>() {
      @Override
      public void handle(final String address) {
        if (replicaMap.containsKey(address)) {
          replicaMap.get(address).sync(index, new Handler<Void>() {
            @Override
            public void handle(Void _) {
              majority.succeed(address);
            }
          });
        }
      }
    }, new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        majorities.remove(majority);
        doneHandler.handle((Void) null);
      }
    });
  }

  /**
   * Pings a majority of the cluster.
   */
  private void readMajority(final Handler<Void> doneHandler) {
    final Majority majority = new Majority(members);
    majorities.add(majority);
    majority.start(new Handler<String>() {
      @Override
      public void handle(final String address) {
        if (replicaMap.containsKey(address)) {
          replicaMap.get(address).ping(new Handler<Void>() {
            @Override
            public void handle(Void _) {
              majority.succeed(address);
            }
          });
        }
      }
    }, new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        majorities.remove(majority);
        doneHandler.handle((Void) null);
      }
    });
  }

  private class Replica {
    private final String address;
    private Long nextIndex;
    private long matchIndex;
    private boolean updating;
    private Map<Long, Handler<Void>> updateHandlers = new HashMap<>();

    private Replica(String address) {
      this.address = address;
    }

    private void update() {
      if (updating) {
        return;
      }
      updating = true;

      log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.succeeded()) {
            if (nextIndex == null) {
              nextIndex =  result.result() + 1;
            }
            if (nextIndex <= result.result()) {
              sync();
            }
            else {
              ping();
            }
          }
        }
      });
    }

    private void ping() {
      endpoint.ping(address, new PingRequest(context.currentTerm(), context.address()),
      context.electionTimeout() / 2, new Handler<AsyncResult<PingResponse>>() {
        @Override
        public void handle(AsyncResult<PingResponse> result) {
          // If the replica returned a newer term then step down.
          if (result.succeeded() && result.result().term() > context.currentTerm()) {
            context.transition(StateType.FOLLOWER);
          }
          updating = false;
        }
      });
    }

    private void ping(final Handler<Void> doneHandler) {
      endpoint.ping(address, new PingRequest(context.currentTerm(), context.address()),
      context.electionTimeout() / 2, new Handler<AsyncResult<PingResponse>>() {
        @Override
        public void handle(AsyncResult<PingResponse> result) {
          // If the replica returned a newer term then step down.
          if (result.succeeded() && result.result().term() > context.currentTerm()) {
            context.transition(StateType.FOLLOWER);
          }
          doneHandler.handle((Void) null);
        }
      });
    }

    private void sync(final long index, final Handler<Void> doneHandler) {
      lock.acquire(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          log.lastIndex(new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              lock.release();
              if (result.failed()) {
                vertx.runOnContext(new Handler<Void>() {
                  @Override
                  public void handle(Void _) {
                    sync(index, doneHandler);
                  }
                });
              }
              else if (nextIndex <= result.result()) {
                updateHandlers.put(index, doneHandler);
                sync();
              }
              else {
                doneHandler.handle((Void) null);
              }
            }
          });
        }
      });
    }

    private void sync() {
      if (updating) {
        lock.acquire(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            final long index = nextIndex;
            log.entry(nextIndex, new Handler<AsyncResult<Entry>>() {
              @Override
              public void handle(AsyncResult<Entry> result) {
                if (result.succeeded()) {
                  log.entry(nextIndex, new Handler<AsyncResult<Entry>>() {
                    @Override
                    public void handle(AsyncResult<Entry> result) {
                      if (result.succeeded()) {
                        System.out.println(Leader.this.context.address() + " replicating " + result.result().type().getName() + " entry " + nextIndex + " to " + address);
                        final Entry entry = result.result();
                        if (nextIndex-1 >= 0) {
                          log.entry(nextIndex-1, new Handler<AsyncResult<Entry>>() {
                            @Override
                            public void handle(AsyncResult<Entry> result) {
                              lock.release();
                              if (result.succeeded()) {
                                final long lastLogTerm = result.result().term();
                                endpoint.sync(address, new SyncRequest(context.currentTerm(), context.address(), nextIndex-1, (nextIndex - 1) >= 0 ? lastLogTerm : -1, entry, context.commitIndex()),
                                    context.electionTimeout() / 2, new Handler<AsyncResult<SyncResponse>>() {
                                  @Override
                                  public void handle(AsyncResult<SyncResponse> result) {
                                    // If the request failed then wait to retry.
                                    if (result.failed()) {
                                      updating = false;
                                    }
                                    // If the follower returned a higher term then step down.
                                    else if (result.result().term() > context.currentTerm()) {
                                      context.currentTerm(result.result().term());
                                      context.transition(StateType.FOLLOWER);
                                    }
                                    // If the log entry failed, decrement next index and retry.
                                    else if (!result.result().success()) {
                                      if (nextIndex - 1 == -1) {
                                        updating = false;System.out.println("YES");
                                        context.transition(StateType.FOLLOWER);
                                      }
                                      else {
                                        nextIndex--;
                                        sync();
                                      }
                                    }
                                    // If the log entry succeeded, continue to the next entry.
                                    else {
                                      nextIndex++;
                                      matchIndex++;
                                      if (updateHandlers.containsKey(index)) {
                                        updateHandlers.remove(index).handle((Void) null);
                                      }
                                      checkCommits();
                                      log.lastIndex(new Handler<AsyncResult<Long>>() {
                                        @Override
                                        public void handle(AsyncResult<Long> result) {
                                          if (result.succeeded() && nextIndex <= result.result()) {
                                            sync();
                                          }
                                          else {
                                            updating = false;
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
                        else {
                          lock.release();
                          endpoint.sync(address, new SyncRequest(context.currentTerm(), context.address(), nextIndex-1, -1, entry, context.commitIndex()),
                              context.electionTimeout() / 2, new Handler<AsyncResult<SyncResponse>>() {
                            @Override
                            public void handle(AsyncResult<SyncResponse> result) {
                              // If the request failed then wait to retry.
                              if (result.failed()) {
                                updating = false;
                              }
                              // If the follower returned a higher term then step down.
                              else if (result.result().term() > context.currentTerm()) {
                                context.currentTerm(result.result().term());
                                context.transition(StateType.FOLLOWER);
                              }
                              // If the log entry failed, decrement next index and retry.
                              else if (!result.result().success()) {
                                if (nextIndex - 1 == -1) {
                                  updating = false;
                                  context.transition(StateType.FOLLOWER);
                                }
                                else {
                                  nextIndex--;
                                  sync();
                                }
                              }
                              // If the log entry succeeded, continue to the next entry.
                              else {
                                nextIndex++;
                                matchIndex++;
                                if (updateHandlers.containsKey(index)) {
                                  updateHandlers.remove(index).handle((Void) null);
                                }
                                checkCommits();
                                log.lastIndex(new Handler<AsyncResult<Long>>() {
                                  @Override
                                  public void handle(AsyncResult<Long> result) {
                                    if (result.succeeded() && nextIndex <= result.result()) {
                                      sync();
                                    }
                                    else {
                                      updating = false;
                                    }
                                  }
                                });
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
        });
      }
    }

    @Override
    public String toString() {
      return address;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Replica && ((Replica) other).address.equals(address);
    }

    @Override
    public int hashCode() {
      return address.hashCode();
    }
  }

}
