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
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.NoOpEntry;
import net.kuujo.copycat.log.SnapshotEntry;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;
import net.kuujo.copycat.util.Quorum;

/**
 * A leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Leader extends BaseState implements Observer {
  private static final int BATCH_SIZE = 10;
  private static final Logger logger = Logger.getLogger(Leader.class.getCanonicalName());
  private final Timer pingTimer = new Timer();
  private final TimerTask pingTimerTask = new TimerTask() {
    @Override
    public void run() {
      for (Replica replica : replicas) {
        replica.update();
      }
    }
  };
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap = new HashMap<>();
  private final Set<Quorum> quorums = new HashSet<>();

  Leader(CopyCatContext context) {
    super(context);
  }

  @Override
  void init() {
    // Create a set of replica references in the cluster.
    replicas = new ArrayList<>();
    for (String address : context.stateCluster.getRemoteMembers()) {
      Replica replica = new Replica(address);
      replicaMap.put(address, replica);
      replicas.add(replica);
    }

    // Set up a timer for pinging cluster members.
    pingTimer.schedule(pingTimerTask, context.config().getHeartbeatInterval());

    // Immediately commit a NOOP entry to the log.
    context.log.appendEntry(new NoOpEntry(context.getCurrentTerm()));

    // Once the no-op entry has been appended, immediately update all nodes.
    pingTimerTask.run();

    // If the cluster configuration is observable, observe the configuration
    // for changes.
    if (context.cluster instanceof Observable) {
      ((Observable) context.cluster).addObserver(this);
    }
    clusterChanged(context.cluster);

    // Set this node as the current cluster leader.
    context.setCurrentLeader(context.cluster().getLocalMember());
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
    // Create a set of combined cluster membership between the old configuration
    // and the new/updated configuration.
    final Set<String> combinedMembers = new HashSet<>(context.stateCluster.getMembers());
    combinedMembers.addAll(members);

    long index = context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), combinedMembers));

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

    // Replicate the combined configuration to a quorum of the cluster.
    writeQuorum(index, new AsyncCallback<Boolean>() {
      @Override
      public void complete(Boolean succeeded) {
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
          long index = context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members));

          // Replicate the final configuration to a quorum of the cluster.
          writeQuorum(index, new AsyncCallback<Boolean>() {
            @Override
            public void complete(Boolean succeeded) {
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
              } else {
                context.transition(CopyCatState.FOLLOWER);
              }
            }
            @Override
            public void fail(Throwable t) {
              context.transition(CopyCatState.FOLLOWER);
            }
          });
        } else {
          context.transition(CopyCatState.FOLLOWER);
        }
      }
      @Override
      public void fail(Throwable t) {
        context.transition(CopyCatState.FOLLOWER);
      }
    });
  }

  @Override
  void ping(PingRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.respond(context.getCurrentTerm());
  }

  @Override
  void sync(final SyncRequest request) {
    // If a newer term was provided by the request then sync as normal
    // and then step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      boolean result = doSync(request);

      // Once the new entries have been synchronized, step down.
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
      request.respond(context.getCurrentTerm(), result);
    } else {
      // Otherwise, we must have received some sync request from a node
      // that *thinks* it's the leader, but boy does it have another thing coming!
      // BOOM! This false reply will show that node who's boss!
      request.respond(context.getCurrentTerm(), false);
    }
  }

  @Override
  void install(final InstallRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentLeader(request.leader());
      context.setCurrentTerm(request.term());
      context.transition(CopyCatState.FOLLOWER);
    }
    request.respond(context.getCurrentTerm());
  }

  @Override
  void poll(final PollRequest request) {
    boolean result = doPoll(request);
    request.respond(context.getCurrentTerm(), result);
  }

  @Override
  void submit(final SubmitRequest request) {
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
        readQuorum(new AsyncCallback<Boolean>() {
          @Override
          public void complete(Boolean succeeded) {
            if (succeeded) {
              try {
                request.respond(context.stateMachine.applyCommand(request.command(), request.args()));
              } catch (Exception e) {
                request.respond(e);
              }
            } else {
              request.respond("Failed to acquire majority replication.");
            }
          }
          @Override
          public void fail(Throwable t) {
            request.respond(t);
          }
        });
      } else {
        // If read quorums are disabled then simply apply the command to the
        // state machine and return the result.
        try {
          request.respond(context.stateMachine.applyCommand(request.command(), request.args()));
        } catch (Exception e) {
          request.respond(e.getMessage());
        }
      }
    } else {
      // For write commands or for commands for which a type was not
      // explicitly provided the entry must be replicated on a
      // quorum of the cluster prior to responding to the request.
      final long index = context.log.appendEntry(new CommandEntry(context.getCurrentTerm(), request.command(), request.args()));

      // Required write quorums are optional. Users may allow the log
      // to simply be replicated after the command is applied to the state
      // machine and the response is sent.
      if (context.config().isRequireWriteQuorum()) {
        writeQuorum(index, new AsyncCallback<Boolean>() {
          @Override
          public void complete(Boolean succeeded) {
            if (succeeded) {
              try {
                request.respond(context.stateMachine.applyCommand(request.command(), request.args()));
              } catch (Exception e) {
                request.respond(e.getMessage());
              } finally {
                context.setLastApplied(index);
              }
            } else {
              request.respond("Failed to acquire write majority.");
            }
          }
          @Override
          public void fail(Throwable t) {
            request.respond(t);
          }
        });
      } else {
        // If write quorums are disabled then simply apply the command
        // to the state machine and return the result.
        try {
          request.respond(context.stateMachine.applyCommand(request.command(), request.args()));
        } catch (Exception e) {
          request.respond(e.getMessage());
        } finally {
          context.setLastApplied(index);
        }
      }
    }
  }

  /**
   * Creates a new quorum.
   */
  private Quorum createQuorum(final AsyncCallback<Boolean> callback) {
    final Quorum quorum = new Quorum(context.cluster().getQuorumSize());
    quorums.add(quorum);
    quorum.setCallback(new AsyncCallback<Boolean>() {
      @Override
      public void complete(Boolean succeeded) {
        quorums.remove(quorum);
        callback.complete(succeeded);
      }
      @Override
      public void fail(Throwable t) {
        quorums.remove(quorum);
        callback.fail(t);
      }
    });
    quorum.succeed(); // Count the current node in the quorum.
    return quorum;
  }

  /**
   * Replicates the index to a majority of the cluster.
   */
  private void writeQuorum(final long index, final AsyncCallback<Boolean> callback) {
    final Quorum quorum = createQuorum(callback);
    for (String member : context.stateCluster.getRemoteMembers()) {
      Replica replica = replicaMap.get(member);
      if (replica != null) {
        replica.sync(index, new AsyncCallback<Void>() {
          @Override
          public void complete(Void value) {
            quorum.succeed();
          }
          @Override
          public void fail(Throwable t) {
            quorum.fail();
          }
        });
      }
    }
  }

  /**
   * Pings a majority of the cluster.
   */
  private void readQuorum(final AsyncCallback<Boolean> callback) {
    final Quorum quorum = createQuorum(callback);
    for (String member : context.stateCluster.getRemoteMembers()) {
      Replica replica = replicaMap.get(member);
      if (replica != null) {
        replica.ping(new AsyncCallback<Void>() {
          @Override
          public void complete(Void value) {
            quorum.succeed();
          }
          @Override
          public void fail(Throwable t) {
            quorum.fail();
          }
        });
      }
    }
  }

  @Override
  void destroy() {
    // Cancel the ping timer.
    pingTimer.cancel();

    // Cancel all quorum input attempts.
    for (Quorum quorum : quorums) {
      quorum.cancel();
    }

    // Shut down all replicas.
    for (Replica replica : replicas) {
      replica.shutdown();
    }

    // Stop observing the observable cluster configuration.
    if (context.cluster instanceof Observable) {
      ((Observable) context.cluster).deleteObserver(this);
    }
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
    private final Map<Long, AsyncCallback<Void>> callbacks = new HashMap<>();

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
        long lastIndex = context.log.lastIndex();
        if (nextIndex == null) {
          nextIndex = lastIndex + 1;
        }
        if (nextIndex <= lastIndex) {
          sync();
        } else {
          ping();
        }
      }
    }

    /**
     * Synchronizes the replica to a specific index.
     */
    private void sync(final long index, final AsyncCallback<Void> callback) {
      callbacks.put(index, callback);
      sync();
    }

    /**
     * Synchronizes the replica.
     */
    private void sync() {
      if (!running && !shutdown) {
        running = true;
        doSync();
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

      long lastIndex = context.log.lastIndex();
      if (nextIndex <= lastIndex || matchIndex < context.getCommitIndex()) {
        if (nextIndex - 1 > 0) {
          long prevIndex = nextIndex - 1;
          Entry prevEntry = context.log.getEntry(prevIndex);
          if (prevEntry == null) {
            nextIndex--;
            doSync();
          } else {
            doSync(prevIndex, prevEntry.term(), lastIndex);
          }
        } else {
          doSync(0, 0, lastIndex);
        }
      } else {
        running = false;
      }
    }

    /**
     * Prepares a synchronization request by loading a set of entries to synchronize.
     */
    private void doSync(final long prevIndex, final long prevTerm, final long lastIndex) {
      if (shutdown) {
        running = false;
        return;
      }

      // If there are entries to be synced then load and synchronize the entries.
      if (prevIndex + 1 <= lastIndex) {
        doSync(prevIndex, prevTerm, context.log.getEntries(prevIndex + 1, prevIndex + 1 + BATCH_SIZE > lastIndex ? lastIndex : prevIndex + 1 + BATCH_SIZE), context.getCommitIndex());
      } else {
        // Otherwise, sync the commit index only.
        doSync(prevIndex, prevTerm, new ArrayList<Entry>(), context.getCommitIndex());
      }
    }

    /**
     * Builds and sends a synchronization request to the replica.
     */
    private void doSync(final long prevIndex, final long prevTerm, final List<Entry> entries, final long commitIndex) {
      if (shutdown) {
        running = false;
        return;
      }

      // Replace any snapshot entries with no-op entries. Snapshot entries are not
      // replicated to other logs. Each node is responsible for snapshotting its
      // own state.
      for (int i = 0; i < entries.size(); i++) {
        Entry entry = entries.get(i);
        if (entry instanceof SnapshotEntry) {
          entries.set(i, new NoOpEntry(entry.term()));
        }
      }

      if (logger.isLoggable(Level.FINER)) {
        if (!entries.isEmpty()) {
          if (entries.size() > 1) {
            logger.finer(String.format("%s replicating entries %d-%d to %s", context.cluster().getLocalMember(), prevIndex+1, prevIndex+entries.size(), address));
          } else {
            logger.finer(String.format("%s replicating entry %d to %s", context.cluster().getLocalMember(), prevIndex+1, address));
          }
        } else {
          logger.finer(String.format("%s committing entry %d to %s", context.cluster().getLocalMember(), commitIndex, address));
        }
      }

      context.protocol.sync(address, new SyncRequest(context.getCurrentTerm(), context.cluster().getLocalMember(), prevIndex, prevTerm, entries, commitIndex),
          context.config().getHeartbeatInterval() / 2, new AsyncCallback<SyncResponse>() {
        @Override
        public void complete(SyncResponse response) {
          if (response.success()) {
            if (logger.isLoggable(Level.FINER)) {
              if (!entries.isEmpty()) {
                if (entries.size() > 1) {
                  logger.finer(String.format("%s successfully replicated entries %d-%d to %s", context.cluster().getLocalMember(), prevIndex+1, prevIndex+entries.size(), address));
                } else {
                  logger.finer(String.format("%s successfully replicated entry %d to %s", context.cluster().getLocalMember(), prevIndex+1, address));
                }
              } else {
                logger.finer(String.format("%s successfully committed entry %d to %s", context.cluster().getLocalMember(), commitIndex, address));
              }
            }

            // Update the next index to send and the last index known to be replicated.
            nextIndex = Math.max(nextIndex + 1, prevIndex + entries.size() + 1);
            matchIndex = Math.max(matchIndex, prevIndex + entries.size());

            // Trigger any futures related to the replicated entries.
            for (long i = prevIndex+1; i < (prevIndex+1) + entries.size(); i++) {
              if (callbacks.containsKey(i)) {
                callbacks.remove(i).complete(null);
              }
            }

            // Update the current commit index and continue the synchronization.
            checkCommits();
            doSync();
          } else {
            if (logger.isLoggable(Level.FINER)) {
              if (!entries.isEmpty()) {
                if (entries.size() > 1) {
                  logger.finer(String.format("%s failed to replicate entries %d-%d to %s", context.cluster().getLocalMember(), prevIndex+1, prevIndex+entries.size(), address));
                } else {
                  logger.finer(String.format("%s failed to replicate entry %d to %s", context.cluster().getLocalMember(), prevIndex+1, address));
                }
              } else {
                logger.finer(String.format("%s failed to commit entry %d to %s", context.cluster().getLocalMember(), commitIndex, address));
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
              if (!entries.isEmpty() || prevIndex == commitIndex) {
                nextIndex--;
              }
              checkCommits();
              doSync();
            }
          }
        }
        @Override
        public void fail(Throwable t) {
          running = false;
          if (callbacks.containsKey(nextIndex)) {
            callbacks.remove(nextIndex).fail(t);
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
    private void ping(final AsyncCallback<Void> callback) {
      if (!shutdown) {
        final long startTime = System.currentTimeMillis();
        long timeout = context.config().isUseAdaptiveTimeouts() ? (lastPingTime > 0 ? (long) (lastPingTime * context.config().getAdaptiveTimeoutThreshold()) : context.config().getHeartbeatInterval() / 2) : context.config().getHeartbeatInterval() / 2;
        context.protocol.ping(address, new PingRequest(context.getCurrentTerm(), context.cluster().getLocalMember()), timeout, new AsyncCallback<PingResponse>() {
          @Override
          public void complete(PingResponse response) {
            lastPingTime = System.currentTimeMillis() - startTime;
            if (response.term() > context.getCurrentTerm()) {
              context.transition(CopyCatState.FOLLOWER);
            } else {
              callback.complete(null);
            }
          }
          @Override
          public void fail(Throwable t) {
            callback.fail(t);
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
