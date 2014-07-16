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
import net.kuujo.copycat.log.SnapshotEntry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncLock;
import net.kuujo.copycat.util.Quorum;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Leader extends BaseState implements Observer {
  private static final int BATCH_SIZE = 10;
  private static final Logger logger = LoggerFactory.getLogger(Leader.class);
  private final AsyncLock lock = new AsyncLock();
  private final AsyncLock configLock = new AsyncLock();
  private long pingTimer;
  private final Set<Long> periodicTimers = new HashSet<>();
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap = new HashMap<>();
  private final Set<Quorum> quorums = new HashSet<>();

  Leader(CopyCatContext context) {
    super(context);
  }

  @Override
  public void startUp(final Handler<AsyncResult<Void>> startHandler) {
    // Create a set of replica references in the cluster.
    replicas = new ArrayList<>();
    for (String address : context.stateCluster.getRemoteMembers()) {
      Replica replica = new Replica(address);
      replicaMap.put(address, replica);
      replicas.add(replica);
    }

    // Set up a timer for pinging cluster members.
    pingTimer = context.vertx.setPeriodic(context.config().getHeartbeatInterval(), new Handler<Long>() {
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
    context.log.appendEntry(new NoOpEntry(context.getCurrentTerm()), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(startHandler);
        } else {
          // Once the no-op entry has been appended, immediately update all nodes.
          for (Replica replica : replicas) {
            replica.update();
          }

          // Observe the cluster configuration for changes.
          if (context.cluster instanceof Observable) {
            ((Observable) context.cluster).addObserver(Leader.this);
          }
          clusterChanged(context.cluster);
          new DefaultFutureResult<Void>((Void) null).setHandler(startHandler);

          // We set the current leader *after* notifying the context that the
          // state has finished startup. This gives the context an opportunity
          // to register event bus handlers and ensure the leader can accept
          // requests from other nodes prior to promoting the current leader.
          context.setCurrentLeader(context.cluster().getLocalMember());
        }
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
        final Set<String> combinedMembers = new HashSet<>(context.stateCluster.getMembers());
        combinedMembers.addAll(members);

        // Append a new configuration entry to the log containing the combined
        // cluster membership.
        context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), combinedMembers), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.succeeded()) {
              final long index = result.result();

              // Replicate the combined configuration to a quorum of the cluster.
              writeQuorum(index, new Handler<Boolean>() {
                @Override
                public void handle(Boolean succeeded) {
                  if (succeeded) {
                    // Once the combined configuration has been replicated, apply the
                    // configuration to the current state (internal state machine).
                    Set<String> remoteMembers = new HashSet<>(combinedMembers);
                    remoteMembers.remove(context.stateCluster.getLocalMember());
                    context.stateCluster.setRemoteMembers(remoteMembers);

                    // Update replica references to reflect the configuration changes.
                    for (String address : context.stateCluster.getRemoteMembers()) {
                      if (!replicaMap.containsKey(address)) {
                        Replica replica = new Replica(address);
                        replica.ping();
                        replicaMap.put(address, replica);
                        replicas.add(replica);
                      }
                    }

                    // Now that the combined configuration has been committed, create
                    // and replicate a final configuration containing only the new membership.
                    context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members), new Handler<AsyncResult<Long>>() {
                      @Override
                      public void handle(AsyncResult<Long> result) {
                        if (result.succeeded()) {
                          final long index = result.result();

                          // Replicate the final configuration to a quorum of the cluster.
                          writeQuorum(index, new Handler<Boolean>() {
                            @Override
                            public void handle(Boolean succeeded) {
                              if (succeeded) {
                                // Once the new configuration has been replicated, apply
                                // the configuration to the current state and update the
                                // last applied index.
                                Set<String> remoteMembers = new HashSet<>(members);
                                remoteMembers.remove(context.stateCluster.getLocalMember());
                                context.stateCluster.setRemoteMembers(remoteMembers);

                                // Iterate through replicas and remove any replicas that
                                // were removed from the cluster.
                                Iterator<Replica> iterator = replicas.iterator();
                                while (iterator.hasNext()) {
                                  Replica replica = iterator.next();
                                  if (!context.stateCluster.getRemoteMembers().contains(replica.address)) {
                                    replica.shutdown();
                                    iterator.remove();
                                    replicaMap.remove(replica.address);
                                  }
                                }

                                // Release the configuration lock.
                                configLock.release();
                              } else {
                                context.vertx.setTimer(1000, new Handler<Long>() {
                                  @Override
                                  public void handle(Long timerID) {
                                    updateClusterConfig(members);
                                  }
                                });
                              }
                            }
                          });
                        }
                      }
                    });
                  } else {
                    context.vertx.setTimer(1000, new Handler<Long>() {
                      @Override
                      public void handle(Long timerID) {
                        updateClusterConfig(members);
                      }
                    });
                  }
                }
              });
            }
          }
        });
      }
    });
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.reply(context.getCurrentTerm());
  }

  @Override
  public void sync(final SyncRequest request) {
    // If a newer term was provided by the request then sync as normal
    // and then step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      // Acquire a lock that prevents the local log from being modified
      // during the sync.
      lock.acquire(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          doSync(request, new Handler<AsyncResult<Boolean>>() {
            @Override
            public void handle(AsyncResult<Boolean> result) {
              // Once the new entries have been synchronized, step down.
              context.setCurrentLeader(request.leader());
              context.setCurrentTerm(request.term());
              context.transition(CopyCatState.FOLLOWER);

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
    } else {
      // Otherwise, we must have received some sync request from a node
      // that *thinks* it's the leader, but boy does it have another thing coming!
      // BOOM! This false reply will show that node who's boss!
      request.reply(context.getCurrentTerm(), false);
    }
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
  }

  @Override
  public void submit(final SubmitRequest request) {
    // Try to determine the type of command this request is executing. The command
    // type is provided by a CommandProvider which provides CommandInfo for a
    // given command. If no CommandInfo is provided then all commands are assumed
    // to be READ_WRITE commands. Depending on the command type, read or write
    // commands may or may not be replicated to a quorum based on configuration
    // options. For write commands, if a quorum is required then the command will
    // be replicated. For read commands, if a quorum is required then we simply
    // ping a quorum of the cluster to ensure that data is not stale.
    CommandInfo info = null;
    if (context.stateMachine instanceof CommandProvider) {
      info = ((CommandProvider) context.stateMachine).getCommandInfo(request.command());
    }

    // If no command info was provided then we default to a READ_WRITE command.
    if (info != null && info.type().equals(CommandInfo.Type.READ)) {

      // Users have the option of whether to require read quorums. If read
      // quorums are disabled then it is simply assumed that there are no
      // log conflicts since the last ping. This is safe in most cases since
      // log conflicts are unlikely in most cases. Nevertheless, read majorities
      // are required by default.
      if (context.config().isRequireReadQuorum()) {
        readQuorum(new Handler<Boolean>() {
          @Override
          public void handle(Boolean succeeded) {
            if (succeeded) {
              try {
                request.reply(context.stateMachine.applyCommand(request.command(), request.args()));
              } catch (Exception e) {
                request.error(e.getMessage());
              }
            } else {
              request.error("Failed to acquire majority replication.");
            }
          }
        });
      } else {
        // If read quorums are disabled then simply apply the command to the
        // state machine and return the result.
        try {
          request.reply(context.stateMachine.applyCommand(request.command(), request.args()));
        } catch (Exception e) {
          request.error(e.getMessage());
        }
      }
    } else {
      // For write commands or for commands for which a type was not
      // explicitly provided the entry must be replicated on a
      // quorum of the cluster prior to responding to the request.
      context.log.appendEntry(new CommandEntry(context.getCurrentTerm(), request.command(), request.args()), new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.failed()) {
            request.error(result.cause());
          } else {
            // Replicate the log entry to a majority of the cluster.
            final long index = result.result();

            // Required write quorums are optional. Users may allow the log
            // to simply be replicated after the command is applied to the state
            // machine and the response is sent.
            if (context.config().isRequireWriteQuorum()) {
              writeQuorum(index, new Handler<Boolean>() {
                @Override
                public void handle(Boolean succeeded) {
                  if (succeeded) {
                    try {
                      JsonObject output = context.stateMachine.applyCommand(request.command(), request.args());
                      request.reply(output);
                    } catch (Exception e) {
                      request.error(e.getMessage());
                    } finally {
                      context.setLastApplied(index);
                    }
                  } else {
                    request.error("Failed to acquire write majority.");
                  }
                }
              });
            } else {
              // If write quorums are disabled then simply apply the command
              // to the state machine and return the result.
              try {
                JsonObject output = context.stateMachine.applyCommand(request.command(), request.args());
                request.reply(output);
              } catch (Exception e) {
                request.error(e.getMessage());
              } finally {
                context.setLastApplied(index);
              }
            }
          }
        }
      });
    }
  }

  /**
   * Creates a new quorum.
   */
  private Quorum createQuorum(final Handler<Boolean> doneHandler) {
    final Quorum quorum = new Quorum(context.cluster().getQuorumSize());
    quorums.add(quorum);
    quorum.setHandler(new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        quorums.remove(quorum);
        doneHandler.handle(succeeded);
      }
    });
    quorum.succeed(); // Count the current node in the quorum.
    return quorum;
  }

  /**
   * Replicates the index to a majority of the cluster.
   */
  private void writeQuorum(final long index, final Handler<Boolean> doneHandler) {
    final Quorum quorum = createQuorum(doneHandler);
    for (String member : context.stateCluster.getRemoteMembers()) {
      Replica replica = replicaMap.get(member);
      if (replica != null) {
        replica.sync(index, new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              quorum.fail();
            } else {
              quorum.succeed();
            }
          }
        });
      }
    }
  }

  /**
   * Pings a majority of the cluster.
   */
  private void readQuorum(final Handler<Boolean> doneHandler) {
    final Quorum quorum = createQuorum(doneHandler);
    for (String member : context.stateCluster.getRemoteMembers()) {
      Replica replica = replicaMap.get(member);
      if (replica != null) {
        replica.ping(new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            if (result.failed()) {
              quorum.fail();
            } else {
              quorum.succeed();
            }
          }
        });
      }
    }
  }

  @Override
  public void shutDown(Handler<AsyncResult<Void>> doneHandler) {
    // Cancel the ping timer.
    if (pingTimer > 0) {
      context.vertx.cancelTimer(pingTimer);
      pingTimer = 0;
    }

    // Cancel any periodic retry timers.
    Iterator<Long> iterator = periodicTimers.iterator();
    while (iterator.hasNext()) {
      context.vertx.cancelTimer(iterator.next());
      iterator.remove();
    }

    // Cancel all quorum input attempts.
    for (Quorum quorum : quorums) {
      quorum.cancel();
    }

    // Shut down all replicas.
    for (Replica replica : replicas) {
      replica.shutdown();
    }

    // Stop observing the cluster configuration.
    if (context.cluster instanceof Observable) {
      ((Observable) context.cluster).deleteObserver(this);
    }
    new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
  }

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    if (!replicas.isEmpty()) {
      // Sort the list of replicas, order by the last index that was replicated
      // to the replcica. This will allow us to determine the median index
      // for all known replicated entries across all cluster members.
      Collections.sort(replicas, new Comparator<Replica>() {
        @Override
        public int compare(Replica o1, Replica o2) {
          return Long.compare(o1.matchIndex, o2.matchIndex);
        }
      });
  
      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the
      // floor(replicas size / 2) to get the middle most index. This
      // will ensure we get the replica which contains the log index
      // for the highest entry that has been replicated to a quorum
      // of the cluster.
      context.setCommitIndex(replicas.get((int) Math.floor(replicas.size() / 2)).matchIndex);
    }
  }

  /**
   * A replica reference.
   */
  private class Replica {
    private final String address;
    private Long nextIndex;
    private long matchIndex;
    private long lastPingTime;
    private boolean running;
    private boolean shutdown;
    private final Map<Long, Future<Void>> futures = new HashMap<>();

    private Replica(String address) {
      this.address = address;
      this.matchIndex = 0;
    }

    /**
     * Updates the replica, either synchronizing new log entries to the replica
     * or by pinging the replica.
     */
    private void update() {
      if (!shutdown) {
        context.log.lastIndex(new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (nextIndex == null) {
              nextIndex = result.result() + 1;
            }
            if (result.succeeded()) {
              if (nextIndex <= result.result()) {
                sync();
              } else {
                ping();
              }
            }
          }
        });
      }
    }

    /**
     * Synchronizes the replica to a specific index.
     */
    private void sync(final long index, final Handler<AsyncResult<Void>> doneHandler) {
      futures.put(index, new DefaultFutureResult<Void>().setHandler(doneHandler));
      sync();
    }

    /**
     * Synchronizes the replica.
     */
    private void sync() {
      if (!running && !shutdown) {
        running = true;
        if (nextIndex == null) {
          context.log.lastIndex(new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              if (result.succeeded()) {
                nextIndex = result.result() + 1;
                doSync();
              } else {
                running = false;
              }
            }
          });
        } else {
          doSync();
        }
      }
    }

    /**
     * Starts synchronizing the replica by loading the last log entry.
     */
    private void doSync() {
      if (shutdown) {
        running = false;
        return;
      }

      context.log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.succeeded()) {
            final long lastIndex = result.result();
            if (nextIndex == null) {
              nextIndex = lastIndex + 1;
            }
            if (nextIndex <= lastIndex || matchIndex < context.getCommitIndex()) {
              if (nextIndex-1 > 0) {
                final long prevLogIndex = nextIndex - 1;
                context.log.getEntry(prevLogIndex, new Handler<AsyncResult<Entry>>() {
                  @Override
                  public void handle(AsyncResult<Entry> result) {
                    if (result.failed()) {
                      running = false;
                    } else if (result.result() == null) {
                      nextIndex--;
                      doSync();
                    } else {
                      doSync(prevLogIndex, result.result().term(), lastIndex);
                    }
                  }
                });
              } else {
                doSync(0, 0, lastIndex);
              }
            } else {
              running = false;
            }
          }
        }
      });
    }

    /**
     * Prepares a synchronization request by loading a set of entries to synchronize.
     */
    private void doSync(final long prevLogIndex, final long prevLogTerm, final long lastIndex) {
      if (shutdown) {
        running = false;
        return;
      }

      // If there are entries to be synced then load the entries.
      if (prevLogIndex+1 <= lastIndex) {
        context.log.getEntries(prevLogIndex+1, (prevLogIndex+1) + BATCH_SIZE > lastIndex ? lastIndex : (prevLogIndex+1) + BATCH_SIZE, new Handler<AsyncResult<List<Entry>>>() {
          @Override
          public void handle(AsyncResult<List<Entry>> result) {
            if (result.failed()) {
              running = false;
            } else {
              doSync(prevLogIndex, prevLogTerm, result.result(), context.getCommitIndex());
            }
          }
        });
      } else {
        // Otherwise, sync the commit index only.
        doSync(prevLogIndex, prevLogTerm, new ArrayList<Entry>(), context.getCommitIndex());
      }
    }

    /**
     * Builds and sends a synchronization request to the replica.
     */
    private void doSync(final long prevLogIndex, final long prevLogTerm, final List<Entry> entries, final long commitIndex) {
      if (shutdown) {
        running = false;
        return;
      }

      // Replce any snapshot entries with no-op entries. Snapshot entries are not
      // replicated to other logs. Each node is responsible for snapshotting its
      // own state.
      for (int i = 0; i < entries.size(); i++) {
        Entry entry = entries.get(i);
        if (entry instanceof SnapshotEntry) {
          entries.set(i, new NoOpEntry(entry.term()));
        }
      }

      if (logger.isDebugEnabled()) {
        if (!entries.isEmpty()) {
          if (entries.size() > 1) {
            logger.debug(String.format("%s replicating entries %d-%d to %s", context.cluster().getLocalMember(), prevLogIndex+1, prevLogIndex+entries.size(), address));
          } else {
            logger.debug(String.format("%s replicating entry %d to %s", context.cluster().getLocalMember(), prevLogIndex+1, address));
          }
        } else {
          logger.debug(String.format("%s committing entry %d to %s", context.cluster().getLocalMember(), commitIndex, address));
        }
      }

      context.client.sync(address, new SyncRequest(context.getCurrentTerm(), context.cluster().getLocalMember(), prevLogIndex, prevLogTerm, entries, commitIndex),
          context.config().getHeartbeatInterval() / 2, new Handler<AsyncResult<SyncResponse>>() {
        @Override
        public void handle(AsyncResult<SyncResponse> result) {
          if (result.succeeded()) {
            if (result.result().success()) {
              if (logger.isDebugEnabled()) {
                if (!entries.isEmpty()) {
                  if (entries.size() > 1) {
                    logger.debug(String.format("%s successfully replicated entries %d-%d to %s", context.cluster().getLocalMember(), prevLogIndex+1, prevLogIndex+entries.size(), address));
                  } else {
                    logger.debug(String.format("%s successfully replicated entry %d to %s", context.cluster().getLocalMember(), prevLogIndex+1, address));
                  }
                } else {
                  logger.debug(String.format("%s successfully committed entry %d to %s", context.cluster().getLocalMember(), commitIndex, address));
                }
              }

              // Update the next index to send and the last index known to be replicated.
              nextIndex = Math.max(nextIndex + 1, prevLogIndex + entries.size() + 1);
              matchIndex = Math.max(matchIndex, prevLogIndex + entries.size());

              // Trigger any futures related to the replicated entries.
              for (long i = prevLogIndex+1; i < (prevLogIndex+1) + entries.size(); i++) {
                if (futures.containsKey(i)) {
                  futures.remove(i).setResult((Void) null);
                }
              }

              // Update the current commit index and continue the synchronization.
              checkCommits();
              doSync();
            } else {
              if (logger.isDebugEnabled()) {
                if (!entries.isEmpty()) {
                  if (entries.size() > 1) {
                    logger.debug(String.format("%s failed to replicate entries %d-%d to %s", context.cluster().getLocalMember(), prevLogIndex+1, prevLogIndex+entries.size(), address));
                  } else {
                    logger.debug(String.format("%s failed to replicate entry %d to %s", context.cluster().getLocalMember(), prevLogIndex+1, address));
                  }
                } else {
                  logger.debug(String.format("%s failed to commit entry %d to %s", context.cluster().getLocalMember(), commitIndex, address));
                }
              }

              // If replication failed then decrement the next index and attemt to
              // retry replication. If decrementing the next index would result in
              // a next index of 0 then something must have gone wrong. Revert to
              // a follower.
              if (nextIndex-1 == 0) {
                running = false;
                context.transition(CopyCatState.FOLLOWER);
              } else {
                // If we were attempting to replicate log entries and not just
                // sending a commit index or if we didn't have any log entries
                // to replicate then decrement the next index. The node we were
                // attempting to sync is not up to date.
                if (!entries.isEmpty() || prevLogIndex == commitIndex) {
                  nextIndex--;
                }
                checkCommits();
                doSync();
              }
            }
          } else {
            running = false;
            if (futures.containsKey(nextIndex)) {
              futures.remove(nextIndex).setFailure(result.cause());
            }
          }
        }
      });
    }

    /**
     * Pings the replica.
     */
    private void ping() {
      ping(null);
    }

    /**
     * Pings the replica, calling a handler once complete.
     */
    private void ping(final Handler<AsyncResult<Void>> doneHandler) {
      if (!shutdown) {
        final long startTime = System.currentTimeMillis();
        context.client.ping(address, new PingRequest(context.getCurrentTerm(), context.cluster().getLocalMember()),
            context.config().isUseAdaptiveTimeouts() ? (lastPingTime > 0 ? (long) (lastPingTime * context.config().getAdaptiveTimeoutThreshold()) : context.config().getHeartbeatInterval() / 2) : context.config().getHeartbeatInterval() / 2,
                new Handler<AsyncResult<PingResponse>>() {
          @Override
          public void handle(AsyncResult<PingResponse> result) {
            if (result.succeeded()) {
              lastPingTime = System.currentTimeMillis() - startTime;
              if (result.result().term() > context.getCurrentTerm()) {
                context.transition(CopyCatState.FOLLOWER);
              } else {
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
            } else {
              new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
            }
          }
        });
      }
    }

    /**
     * Shuts down the replica.
     */
    private void shutdown() {
      shutdown = true;
    }
  }

}
