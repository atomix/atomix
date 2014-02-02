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
package net.kuujo.copycat.replication.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.NoOpEntry;
import net.kuujo.copycat.replication.protocol.PingRequest;
import net.kuujo.copycat.replication.protocol.PingResponse;
import net.kuujo.copycat.replication.protocol.PollRequest;
import net.kuujo.copycat.replication.protocol.SubmitRequest;
import net.kuujo.copycat.replication.protocol.SyncRequest;
import net.kuujo.copycat.replication.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A leader state.
 * 
 * @author Jordan Halterman
 */
public class Leader extends BaseState implements Observer {
  private static final Logger logger = LoggerFactory.getLogger(Leader.class);
  private final StateLock lock = new StateLock();
  private final StateLock configLock = new StateLock();
  private long pingTimer;
  private final Set<Long> periodicTimers = new HashSet<>();
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap = new HashMap<>();
  private final Set<Majority> majorities = new HashSet<>();

  @Override
  public void startUp(final Handler<Void> startHandler) {
    // Create a set of replica references in the cluster.
    members = config.getMembers();
    remoteMembers = new HashSet<>(members);
    remoteMembers.remove(context.address());
    replicas = new ArrayList<>();
    for (String address : remoteMembers) {
      Replica replica = new Replica(address);
      replicaMap.put(address, replica);
      replicas.add(replica);
    }

    // Set up a timer for pinging cluster members.
    pingTimer = vertx.setPeriodic(context.heartbeatInterval(), new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        for (Replica replica : replicas) {
          replica.update();
        }
      }
    });

    // Immediately commit a NOOP entry to the log. If the commit fails
    // then we periodically retry appending the entry until successful.
    // The leader cannot start until this no-op entry has been
    // successfully appended.
    periodicRetry(100, new Handler<Handler<Boolean>>() {
      @Override
      public void handle(final Handler<Boolean> doneHandler) {
        log.appendEntry(new NoOpEntry(context.currentTerm()), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.succeeded()) {
              apply(result.result(), new Handler<Void>() {
                @Override
                public void handle(Void _) {
                  // Once the no-op entry has been appended, immediately update
                  // all nodes.
                  for (Replica replica : replicas) {
                    replica.update();
                  }

                  // Observe the cluster configuration for changes.
                  config.addObserver(Leader.this);
                  context.currentLeader(context.address());
                  startHandler.handle((Void) null);
                }
              });
              doneHandler.handle(true);
            }
            else {
              doneHandler.handle(false);
            }
          }
        });
      }
    });
  }

  @Override
  public void update(Observable config, Object arg) {
    clusterChanged((ClusterConfig) config);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(final ClusterConfig config) {
    updateClusterConfig(config.getMembers());
  }

  /**
   * Updates cluster membership in a two-phase process.
   */
  private void updateClusterConfig(final Set<String> members) {
    // Use a lock to ensure that only one configuration change may take place
    // at any given time. This lock is separate from the global state lock.
    configLock.acquire(new Handler<Void>() {
      @Override
      public void handle(Void _) {

        // Create a set of combined cluster membership between the old configuration
        // and the new/updated configuration.
        final Set<String> combinedMembers = new HashSet<>(Leader.this.members);
        combinedMembers.addAll(members);

        // Append a new configuration entry to the log containing the combined
        // cluster membership.
        log.appendEntry(new ConfigurationEntry(context.currentTerm(), combinedMembers), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.succeeded()) {
              final long index = result.result();

              // Replicate the combined configuration to a majority of the cluster.
              writeMajority(index, new Handler<Void>() {
                @Override
                public void handle(Void _) {

                  // Apply the combined configuration to the state.
                  apply(index, new Handler<Void>() {
                    @Override
                    public void handle(Void _) {
                      // Once the combined configuration has been replicated, apply the
                      // configuration to the current state (internal state machine).
                      Leader.this.members = combinedMembers;
                      Leader.this.remoteMembers = new HashSet<>(combinedMembers);
                      Leader.this.remoteMembers.remove(context.address());

                      // Update replica references to reflect the configuration changes.
                      for (String address : Leader.this.remoteMembers) {
                        if (!replicaMap.containsKey(address)) {
                          Replica replica = new Replica(address);
                          replicaMap.put(address, replica);
                          replicas.add(replica);
                        }
                      }

                      // Now that the combined configuration has been committed, create
                      // and replicate a final configuration containing only the new membership.
                      log.appendEntry(new ConfigurationEntry(context.currentTerm(), members), new Handler<AsyncResult<Long>>() {
                        @Override
                        public void handle(AsyncResult<Long> result) {
                          if (result.succeeded()) {
                            final long index = result.result();

                            // Replicate the final configuration to a majority of the cluster.
                            writeMajority(index, new Handler<Void>() {
                              @Override
                              public void handle(Void _) {

                                // Apply the final configuration to the state.
                                apply(index, new Handler<Void>() {
                                  @Override
                                  public void handle(Void _) {
                                    // Once the new configuration has been replicated, apply
                                    // the configuration to the current state and update the
                                    // last applied index.
                                    Leader.this.members = members;
                                    Leader.this.remoteMembers = new HashSet<>(members);
                                    Leader.this.remoteMembers.remove(context.address());

                                    // Iterate through replicas and remove any replicas that
                                    // were removed from the cluster.
                                    Iterator<Replica> iterator = replicas.iterator();
                                    while (iterator.hasNext()) {
                                      Replica replica = iterator.next();
                                      if (!remoteMembers.contains(replica.address)) {
                                        replica.shutdown();
                                        iterator.remove();
                                        replicaMap.remove(replica.address);
                                      }
                                    }

                                    // Release the configuration lock.
                                    configLock.release();
                                  }
                                });
                              }
                            });
                          }
                        }
                      });
                    }
                  });
                }
              });
            }
          }
        });
      }
    });
  }

  /**
   * Periodically retries a handler.
   */
  private void periodicRetry(final long delay, final Handler<Handler<Boolean>> handler) {
    periodicTimers.add(vertx.setPeriodic(delay, new Handler<Long>() {
      @Override
      public void handle(final Long timerID) {
        handler.handle(new Handler<Boolean>() {
          @Override
          public void handle(Boolean succeeded) {
            if (succeeded) {
              vertx.cancelTimer(timerID);
              periodicTimers.remove(timerID);
            }
          }
        });
      }
    }));
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
    // If a newer term was provided by the request then sync as normal
    // and then step down as leader.
    if (request.term() > context.currentTerm()) {
      // Acquire a lock that prevents the local log from being modified
      // during the sync.
      lock.acquire(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          doSync(request, new Handler<AsyncResult<Boolean>>() {
            @Override
            public void handle(AsyncResult<Boolean> result) {
              // Once the new entries have been synchronized, step down.
              context.currentLeader(request.leader());
              context.currentTerm(request.term());
              context.transition(StateType.FOLLOWER);

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
    // Otherwise, we must have received some sync request from a node
    // that *thinks* it's the leader, but boy does it have another thing coming!
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void poll(final PollRequest request) {
    doPoll(request, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          request.error(result.cause());
        }
        else {
          request.reply(context.currentTerm(), result.result());
        }
      }
    });
  }

  @Override
  public void submit(final SubmitRequest request) {
    // If this is a read command then we need to contact a majority of the
    // cluster
    // to ensure that the information is not stale. Once we've determined that
    // this node is the most up-to-date, we can simply apply the command to
    // the
    // state machine and return the result without replicating the log.
    if (request.command().type() != null && request.command().type().isReadOnly()) {
      if (context.requireReadMajority()) {
        readMajority(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            try {
              request.reply(stateMachine.applyCommand(request.command()));
            }
            catch (Exception e) {
              request.error(e.getMessage());
            }
          }
        });
      }
      else {
        try {
          request.reply(stateMachine.applyCommand(request.command()));
        }
        catch (Exception e) {
          request.error(e.getMessage());
        }
      }
    }
    // Otherwise, for write commands or for commands for which a type was not
    // explicitly provided the entry must be replicated on a
    // majority of the cluster prior to responding to the request.
    else {
      // Append a new command entry to the log.
      log.appendEntry(new CommandEntry(context.currentTerm(), request.command()), new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.failed()) {
            request.error(result.cause());
          }
          else {
            // Replicate the log entry to a majority of the cluster.
            final long index = result.result();
            if (context.requireWriteMajority()) {
              writeMajority(index, new Handler<Void>() {
                @Override
                public void handle(Void _) {
                  apply(index, new Handler<Void>() {
                    @Override
                    public void handle(Void _) {
                      try {
                        Object output = stateMachine.applyCommand(request.command());
                        request.reply(output);
                      }
                      catch (Exception e) {
                        request.error(e.getMessage());
                      }
                    }
                  });
                }
              });
            }
            else {
              apply(index, new Handler<Void>() {
                @Override
                public void handle(Void _) {
                  try {
                    Object output = stateMachine.applyCommand(request.command());
                    context.lastApplied(index);
                    request.reply(output);
                  }
                  catch (Exception e) {
                    request.error(e.getMessage());
                  }
                }
              });
            }
          }
        }
      });
    }
  }

  /**
   * Replicates the index to a majority of the cluster.
   */
  private void writeMajority(final long index, final Handler<Void> doneHandler) {
    final Majority majority = new Majority(remoteMembers).countSelf();
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
    final Majority majority = new Majority(remoteMembers).countSelf();
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

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    // Cancel the ping timer.
    if (pingTimer > 0) {
      vertx.cancelTimer(pingTimer);
      pingTimer = 0;
    }

    // Cancel any periodic retry timers.
    Iterator<Long> iterator = periodicTimers.iterator();
    while (iterator.hasNext()) {
      vertx.cancelTimer(iterator.next());
      iterator.remove();
    }

    // Cancel all majority input attempts.
    for (Majority majority : majorities) {
      majority.cancel();
    }

    // Stop observing the cluster configuration.
    config.deleteObserver(this);
    doneHandler.handle((Void) null);
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
    context.commitIndex(replicas.get(middle).matchIndex);
    log.floor(Math.min(context.commitIndex(), context.lastApplied()));
  }

  /**
   * A replica reference.
   */
  private class Replica {
    private final String address;
    private long nextIndex;
    private long matchIndex;
    private final StateLock lock = new StateLock();
    private boolean shutdown;

    private Replica(String address) {
      this.address = address;
      this.matchIndex = 0;
      this.nextIndex = log.lastIndex() + 1;
    }

    private void update() {
      if (!lock.locked() && !shutdown) {
        if (nextIndex <= log.lastIndex() || matchIndex <= nextIndex-1) {
          sync();
        }
        else {
          ping();
        }
      }
    }
 
    private void sync() {
      if (nextIndex <= log.lastIndex() || matchIndex <= nextIndex-1) {
        sync(log.lastIndex(), null);
      }
    }

    private void sync(final long toIndex, final Handler<Void> doneHandler) {
      if (!shutdown) {
        lock.acquire(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            doSync(toIndex, new Handler<Void>() {
              @Override
              public void handle(Void _) {
                lock.release();
                if (doneHandler != null) {
                  doneHandler.handle((Void) null);
                }
              }
            });
          }
        });
      }
    }

    private void doSync(final long toIndex, final Handler<Void> doneHandler) {
      if (shutdown) return;
      if (nextIndex > toIndex) {
        lock.release();
        ping();
        doneHandler.handle((Void) null);
      }
      else {
        if (toIndex <= log.lastIndex()) {
          if (nextIndex - 1 > 0) {
            final long prevLogIndex = nextIndex - 1;
            log.entry(nextIndex-1, new Handler<AsyncResult<Entry>>() {
              @Override
              public void handle(AsyncResult<Entry> result) {
                if (result.failed()) {
                  doSync(toIndex, doneHandler);
                }
                else if (result.result() == null) {
                  nextIndex--;
                  doSync(toIndex, doneHandler);
                }
                else {
                  doSync(toIndex, prevLogIndex, result.result().term(), doneHandler);
                }
              }
            });
          }
          else {
            doSync(toIndex, 0, 0, doneHandler);
          }
        }
      }
    }

    private void doSync(final long toIndex, final long prevLogIndex, final long prevLogTerm, final Handler<Void> doneHandler) {
      if (shutdown) return;
      if (prevLogIndex+1 <= log.lastIndex()) {
        log.entry(prevLogIndex+1, new Handler<AsyncResult<Entry>>() {
          @Override
          public void handle(AsyncResult<Entry> result) {
            if (result.failed()) {
              doSync(toIndex, doneHandler);
            }
            else {
              doSync(toIndex, prevLogIndex, prevLogTerm, result.result(), context.commitIndex(), doneHandler);
            }
          }
        });
      }
      else {
        doSync(toIndex, prevLogIndex, prevLogTerm, null, context.commitIndex(), doneHandler);
      }
    }

    private void doSync(final long toIndex, final long prevLogIndex, final long prevLogTerm, final Entry entry, final long commitIndex, final Handler<Void> doneHandler) {
      if (shutdown) return;
      if (logger.isInfoEnabled()) {
        if (entry != null) {
          logger.info(String.format("%s replicating entry %d to %s", context.address(), prevLogIndex+1, address));
        }
        else {
          logger.info(String.format("%s committing entry %d to %s", context.address(), commitIndex, address));
        }
      }
      endpoint.sync(address, new SyncRequest(context.currentTerm(), context.address(), prevLogIndex, prevLogTerm, entry, commitIndex), new Handler<AsyncResult<SyncResponse>>() {
        @Override
        public void handle(AsyncResult<SyncResponse> result) {
          if (result.succeeded()) {
            if (result.result().success()) {
              logger.info(String.format("%s successfully replicated entry %d to %s", context.address(), prevLogIndex+1, address));
              nextIndex++;
              matchIndex = commitIndex;
              checkCommits();
              if (toIndex < nextIndex) {
                lock.release();
                doneHandler.handle((Void) null);
              }
              else {
                doSync(toIndex, doneHandler);
              }
            }
            else {
              logger.info(String.format("%s failed to replicate entry %d to %s", context.address(), prevLogIndex+1, address));
              if (nextIndex-1 == 0) {
                lock.release();
                context.transition(StateType.FOLLOWER);
              }
              else {
                nextIndex--;
                checkCommits();
                if (toIndex < nextIndex) {
                  lock.release();
                  doneHandler.handle((Void) null);
                }
                else {
                  doSync(toIndex, doneHandler);
                }
              }
            }
          }
          else {
            vertx.setTimer(100, new Handler<Long>() {
              @Override
              public void handle(Long timerID) {
                doSync(toIndex, doneHandler);
              }
            });
          }
        }
      });
    }

    private void ping() {
      ping(null);
    }

    private void ping(final Handler<Void> doneHandler) {
      if (!lock.locked() && !shutdown) {
        endpoint.ping(address, new PingRequest(context.currentTerm(), context.address()), context.heartbeatInterval(), new Handler<AsyncResult<PingResponse>>() {
          @Override
          public void handle(AsyncResult<PingResponse> result) {
            if (result.succeeded()) {
              if (result.result().term() > context.currentTerm()) {
                context.transition(StateType.FOLLOWER);
              }
              else if (doneHandler != null) {
                doneHandler.handle((Void) null);
              }
            }
          }
        });
      }
    }

    private void shutdown() {
      shutdown = true;
    }
  }

}
