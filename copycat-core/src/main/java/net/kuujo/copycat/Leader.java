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
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.NoOpEntry;
import net.kuujo.copycat.log.impl.SnapshotEntry;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;
import net.kuujo.copycat.util.AsyncCallback;
import net.kuujo.copycat.util.Quorum;

/**
 * Leader replica state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Leader extends BaseState implements Observer {
  private static final Logger logger = Logger.getLogger(Leader.class.getCanonicalName());
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private final Timer pingTimer = new Timer();
  private TimerTask pingTimerTask;
  private final List<RemoteReplica> replicas = new ArrayList<>();
  private final Map<String, RemoteReplica> replicaMap = new HashMap<>();
  private final TreeMap<Long, Runnable> tasks = new TreeMap<>();

  @Override
  public void init(CopyCatContext context) {
    super.init(context);

    // Set the current leader as this replica.
    context.setCurrentLeader(context.cluster.config().getLocalMember());

    // Set the periodic ping timer.
    setPingTimer();

    // The first thing the leader needs to do is check whether the log is empty.
    // If the log is empty then we immediately commit an empty SnapshotEntry to
    // the log which will be replicated to other nodes if necessary. The first
    // entry in the log must always be a snapshot entry for consistency.
    if (context.log.isEmpty()) {
      Map<String, Object> snapshot = context.stateMachine.createSnapshot();
      byte[] snapshotBytes = serializer.writeValue(snapshot);
      context.log.appendEntry(new SnapshotEntry(context.getCurrentTerm(), context.cluster.config().getMembers(), snapshotBytes, true));
    }

    // Next, the leader must write a no-op entry to the log and replicate the log
    // to all the nodes in the cluster. This ensures that other nodes are notified
    // of the leader's election.
    context.log.appendEntry(new NoOpEntry(context.getCurrentTerm()));

    // Finally, ensure that the cluster configuration is up-to-date and properly
    // replicated by committing the current configuration to the log. This will
    // ensure that nodes' cluster configurations are consistent with the leader's.
    context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), context.cluster.config().getMembers()));

    // Start observing the user provided cluster configuration for changes.
    // When the cluster configuration changes, changes will be committed to the
    // log and replicated according to the Raft specification.
    if (context.cluster() instanceof Observable) {
      ((Observable) context.cluster()).addObserver(this);
    }

    // Immediately commit the current cluster configuration.
    Set<String> members = new HashSet<>(context.cluster.config().getMembers());
    context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members));

    // Immediately after the entry is appended to the log, apply the combined
    // membership. Cluster membership changes do not wait for commitment.
    Set<String> remoteMembers = new HashSet<>(members);
    remoteMembers.remove(context.cluster.config().getLocalMember());
    context.cluster.config().setRemoteMembers(remoteMembers);

    // Whenever the local cluster membership changes, ensure we update the
    // local replicas.
    for (String member : context.cluster.config().getRemoteMembers()) {
      if (!replicaMap.containsKey(member)) {
        RemoteReplica replica = new RemoteReplica(member);
        replicaMap.put(member, replica);
        replicas.add(replica);
      }
    }

    // Once the initialization entries have been logged and the cluster configuration
    // has been updated, update all nodes in the cluster.
    for (RemoteReplica replica : replicas) {
      replica.update();
    }
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged((ClusterConfig) o);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(final ClusterConfig cluster) {
    // All cluster configuration changes must go through the leader. In order to
    // perform cluster configuration changes, the leader observes the local cluster
    // configuration if it is indeed observable. When a configuration change is
    // detected, in order to ensure log consistency we need to perform a two-step
    // configuration change:
    // - First log the combined current cluster configuration and new cluster
    // configuration. For instance, if a node was added to the cluster, log the
    // new configuration. If a node was removed, log the old configuration.
    // - Once the combined cluster configuration has been replicated, log and
    // sync the new configuration.
    // This two-step process ensures log consistency by ensuring that two majorities
    // cannot result from adding and removing too many nodes at once.

    // First, store the set of members in a final local variable. We don't want
    // to be calling getMembers() at a later time after it has changed again.
    final Set<String> members = cluster.getMembers();

    // If another cluster configuration change is occurring right now, it's possible
    // that the two configuration changes could overlap one another. In order to
    // avoid this, we wait until all entries up to the current log index have been
    // committed before beginning the configuration change. This ensures that any
    // previous configuration changes have completed.
    addTask(context.log.lastIndex(), new Runnable() {
      @Override
      public void run() {
        // First we need to commit a combined old/new cluster configuration entry.
        Set<String> combinedMembers = new HashSet<>(context.cluster.config().getMembers());
        combinedMembers.addAll(members);
        Entry entry = new ConfigurationEntry(context.getCurrentTerm(), combinedMembers);
        long index = context.log.appendEntry(entry);

        // Immediately after the entry is appended to the log, apply the combined
        // membership. Cluster membership changes do not wait for commitment.
        Set<String> combinedRemoteMembers = new HashSet<>(combinedMembers);
        combinedRemoteMembers.remove(context.cluster.config().getLocalMember());
        context.cluster.config().setRemoteMembers(combinedMembers);

        // Whenever the local cluster membership changes, ensure we update the
        // local replicas.
        for (String member : combinedRemoteMembers) {
          if (!replicaMap.containsKey(member)) {
            RemoteReplica replica = new RemoteReplica(member);
            replicaMap.put(member, replica);
            replicas.add(replica);
            replica.ping();
          }
        }

        // Once the entry has been committed we can be sure that it's safe to
        // log and replicate the new cluster configuration.
        addTask(index, new Runnable() {
          @Override
          public void run() {
            // Append the new configuration entry to the log and force all replicas
            // to be synchronized.
            context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members));

            // Again, once we've appended the new configuration to the log, update
            // the local internal configuration.
            Set<String> remoteMembers = new HashSet<>(members);
            remoteMembers.remove(context.cluster.config().getLocalMember());
            context.cluster.config().setRemoteMembers(remoteMembers);

            // Once the local internal configuration has been updated, remove
            // any replicas that are no longer in the cluster.
            Iterator<RemoteReplica> iterator = replicas.iterator();
            while (iterator.hasNext()) {
              RemoteReplica replica = iterator.next();
              if (!remoteMembers.contains(replica.address)) {
                replica.shutdown();
                iterator.remove();
                replicaMap.remove(replica.address);
              }
            }
          }
        });
      }
    });
  }

  /**
   * Resets the ping timer.
   */
  private void setPingTimer() {
    pingTimerTask = new TimerTask() {
      @Override
      public void run() {
        for (RemoteReplica replica : replicas) {
          replica.update();
        }
        setPingTimer();
      }
    };
    pingTimer.schedule(pingTimerTask, context.config().getHeartbeatInterval());
  }

  @Override
  public void ping(PingRequest request, AsyncCallback<PingResponse> responseCallback) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(request.leader());
      context.transition(Follower.class);
    }
    responseCallback.complete(new PingResponse(context.getCurrentTerm()));
  }

  @Override
  public void submit(final SubmitRequest request, final AsyncCallback<SubmitResponse> responseCallback) {
    // Try to determine the type of command this request is executing. The command
    // type is provided by a CommandProvider which provides CommandInfo for a
    // given command. If no CommandInfo is provided then all commands are assumed
    // to be READ_WRITE commands.
    CommandInfo info = context.stateMachine instanceof CommandProvider
        ? ((CommandProvider) context.stateMachine).getCommandInfo(request.command()) : null;

    // Depending on the command type, read or write
    // commands may or may not be replicated to a quorum based on configuration
    // options. For write commands, if a quorum is required then the command will
    // be replicated. For read commands, if a quorum is required then we simply
    // ping a quorum of the cluster to ensure that data is not stale.
    if (info != null && info.type().equals(CommandInfo.Type.READ)) {
      // Users have the option of whether to allow stale data to be returned. By
      // default, read quorums are enabled. If read quorums are disabled then we
      // simply apply the command, otherwise we need to ping a quorum of the
      // cluster to ensure that data is up-to-date before responding.
      if (context.config().isRequireReadQuorum()) {
        final Quorum quorum = new Quorum(context.cluster.config().getQuorumSize());
        quorum.setCallback(new AsyncCallback<Boolean>() {
          @Override
          public void complete(Boolean succeeded) {
            if (succeeded) {
              try {
                responseCallback.complete(new SubmitResponse(context.stateMachine.applyCommand(request.command(), request.args())));
              } catch (Exception e) {
                responseCallback.fail(e);
              }
            } else {
              responseCallback.fail(new CopyCatException("Failed to acquire read quorum"));
            }
          }
          @Override
          public void fail(Throwable t) {
            responseCallback.fail(t);
          }
        });
        for (RemoteReplica replica : replicas) {
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
      } else {
        try {
          responseCallback.complete(new SubmitResponse(context.stateMachine.applyCommand(request.command(), request.args())));
        } catch (Exception e) {
          responseCallback.fail(e);
        }
      }
    } else {
      // For write commands or for commands for which the type is not known, an
      // entry must be logged, replicated, and committed prior to applying it
      // to the state machine and returning the result.
      final long index = context.log.appendEntry(new CommandEntry(context.getCurrentTerm(), request.command(), request.args()));

      // Write quorums are also optional to the user. The user can optionally
      // indicate that write commands should be immediately applied to the state
      // machine and the result returned.
      if (context.config().isRequireWriteQuorum()) {
        // If the replica requires write quorums, we simply set a task to be
        // executed once the entry has been replicated to a quorum of the cluster.
        addTask(index, new Runnable() {
          @Override
          public void run() {
            // Once the entry has been replicated we can apply it to the state
            // machine and respond with the command result.
            try {
              responseCallback.complete(new SubmitResponse(context.stateMachine.applyCommand(request.command(), request.args())));
            } catch (Exception e) {
              responseCallback.fail(e);
            } finally {
              context.setLastApplied(index);
            }
          }
        });
      } else {
        // If write quorums are not required then just apply it and return the
        // result. We don't need to check the order of the application here since
        // all entries written to the log will not require a quorum and thus
        // we won't be applying any entries out of order.
        try {
          responseCallback.complete(new SubmitResponse(context.stateMachine.applyCommand(request.command(), request.args())));
        } catch (Exception e) {
          responseCallback.fail(e);
        } finally {
          context.setLastApplied(index);
        }
      }
    }
  }

  /**
   * Adds a task to be executed once an index has been committed.
   */
  private void addTask(long index, Runnable task) {
    tasks.put(index, task);
  }

  /**
   * Runs all tasks up to a committed index.
   */
  private void runTasks(long index) {
    // Iterate over the tasks map in key order up to the given index, running
    // all tasks during iteration.
    Iterator<Map.Entry<Long, Runnable>> iterator = tasks.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Runnable> entry = iterator.next();
      if (entry.getKey() <= index) {
        entry.getValue().run();
        iterator.remove();
      } else {
        break;
      }
    }
  }

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    if (!replicas.isEmpty()) {
      // Sort the list of replicas, order by the last index that was replicated
      // to the replica. This will allow us to determine the median index
      // for all known replicated entries across all cluster members.
      Collections.sort(replicas, new Comparator<RemoteReplica>() {
        @Override
        public int compare(RemoteReplica o1, RemoteReplica o2) {
          return Long.compare(o1.matchIndex, o2.matchIndex);
        }
      });
  
      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the negation of
      // the required quorum size to get the index of the replica with the least
      // possible quorum replication. That replica's match index is the commit index.
      int index = replicas.size() - context.cluster.config().getQuorumSize();
      if (index > 0) {
        // Set the commit index. Once the commit index has been set we can run
        // all tasks up to the given commit.
        context.setCommitIndex(replicas.get(index).matchIndex);
        runTasks(context.getCommitIndex());
      }
    }
  }

  @Override
  public void destroy() {
    // Cancel the ping timer.
    pingTimer.cancel();

    // Stop observing the observable cluster configuration.
    if (context.cluster() instanceof Observable) {
      ((Observable) context.cluster()).deleteObserver(this);
    }
  }

  /**
   * Represents a reference to a remote replica.
   */
  private class RemoteReplica {
    private final String address;
    private long nextIndex;
    private long matchIndex;
    private boolean running;
    private boolean shutdown;

    public RemoteReplica(String address) {
      this.address = address;
      this.nextIndex = context.log.lastIndex();
    }

    /**
     * Updates the replica, either synchronizing new log entries to the replica
     * or by pinging the replica.
     */
    private void update() {
      if (!shutdown) {
        // If there are entries to be sent to the replica then synchronize,
        // otherwise just ping the replica.
        if (nextIndex <= context.log.lastIndex() || matchIndex < context.getCommitIndex()) {
          sync();
        } else {
          ping();
        }
      }
    }

    /**
     * Synchronizes the log with the replica.
     */
    private void sync() {
      if (!shutdown) {
        if (!running) {
          running = true;
          doSync();
        } else {
          ping();
        }
      }
    }

    /**
     * Begins synchronization with the replica.
     */
    private void doSync() {
      // If the next index to be send to the replica is the first entry in the
      // log then it should be a snapshot. In that case, we need to send an
      // install request to the replica.
      if (nextIndex == context.log.firstIndex()) {
        Entry entry = context.log.getEntry(context.log.firstIndex());
        if (entry instanceof SnapshotEntry) {
          doInstall(context.log.firstIndex(), (SnapshotEntry) entry);
          return;
        }
      }

      // Only perform synchronization if the next index to send is less than
      // or equal to the last log index or if there are entries to be committed.
      if (nextIndex <= context.log.lastIndex() || matchIndex < context.getCommitIndex()) {
        final long prevIndex = nextIndex - 1;
        final Entry prevEntry = context.log.getEntry(prevIndex);

        // Load up to ten entries for replication.
        final List<Entry> entries = context.log.getEntries(nextIndex, nextIndex+10 < context.log.lastIndex() ? nextIndex+10 : context.log.lastIndex());

        final long commitIndex = context.getCommitIndex();
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

        SyncRequest request = new SyncRequest(context.getCurrentTerm(), context.cluster.config().getLocalMember(), prevIndex, prevEntry != null ? prevEntry.term() : 0, entries, commitIndex);
        context.cluster.member(address).protocol().client().sync(request, new AsyncCallback<SyncResponse>() {
          @Override
          public void complete(SyncResponse response) {
            if (response.status().equals(Response.Status.OK)) {
              if (response.succeeded()) {
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
                  context.transition(Follower.class);
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
            } else {
              running = false;
              logger.warning(response.error().getMessage());
            }
          }
          @Override
          public void fail(Throwable t) {
            running = false;
            logger.warning(t.getMessage());
          }
        });
      } else {
        running = false;
        ping();
      }
    }

    /**
     * Installs a snapshot to the remote replica.
     */
    private void doInstall(long index, SnapshotEntry entry) {
      doIncrementalInstall(index, entry.term(), entry.data(), 0, 4096, new AsyncCallback<Void>() {
        @Override
        public void complete(Void value) {
          nextIndex++;
          running = false;
          update();
        }
        @Override
        public void fail(Throwable t) {
          running = false;
        }
      });
    }

    /**
     * Sends incremental install requests to the replica.
     */
    private void doIncrementalInstall(final long index, final long term, final byte[] snapshot, final int position, final int length, final AsyncCallback<Void> callback) {
      if (position == snapshot.length) {
        callback.complete(null);
      } else {
        final int bytesLength = snapshot.length - position > length ? length : snapshot.length - position;
        byte[] bytes = new byte[bytesLength];
        System.arraycopy(snapshot, position, bytes, 0, bytesLength);
        context.cluster.member(address).protocol().client().install(new InstallRequest(context.getCurrentTerm(), context.cluster.config().getLocalMember(), index, term, context.cluster.config().getMembers(), bytes, position + bytesLength == snapshot.length), new AsyncCallback<InstallResponse>() {
          @Override
          public void complete(InstallResponse response) {
            doIncrementalInstall(index, term, snapshot, position + bytesLength, length, callback);
          }
          @Override
          public void fail(Throwable t) {
            callback.fail(t);
          }
        });
      }
    }

    /**
     * Pings the replica.
     */
    private void ping() {
      ping(null);
    }

    /**
     * Pings the replica.
     */
    private void ping(final AsyncCallback<Void> callback) {
      if (!shutdown) {
        context.cluster.member(address).protocol().client().ping(new PingRequest(context.getCurrentTerm(), context.cluster.config().getLocalMember()), new AsyncCallback<PingResponse>() {
          @Override
          public void complete(PingResponse response) {
            if (response.status().equals(Response.Status.OK)) {
              if (response.term() > context.getCurrentTerm()) {
                context.setCurrentTerm(response.term());
                context.setCurrentLeader(null);
                context.transition(Follower.class);
                if (callback != null) {
                  callback.fail(new CopyCatException("Not the leader"));
                }
              } else if (callback != null) {
                callback.complete(null);
              }
            } else if (callback != null) {
              callback.fail(response.error());
            }
          }
          @Override
          public void fail(Throwable t) {
            if (callback != null) {
              callback.fail(t);
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
