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
import java.util.Arrays;
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
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Entries;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.NoOpEntry;
import net.kuujo.copycat.log.impl.SnapshotEntry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.util.Quorum;

/**
 * Leader state.<p>
 *
 * The leader state is assigned to replicas who have assumed
 * a leadership role in the cluster through a cluster-wide election.
 * The leader's role is to receive command submissions and log and
 * replicate state changes. All state changes go through the leader
 * for simplicity.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Leader extends BaseState implements Observer {
  private static final Logger logger = Logger.getLogger(Leader.class.getCanonicalName());
  private ScheduledFuture<Void> currentTimer;
  private final List<RemoteReplica> replicas = new ArrayList<>();
  private final Map<String, RemoteReplica> replicaMap = new HashMap<>();
  private final TreeMap<Long, Runnable> tasks = new TreeMap<>();

  @Override
  public void init(CopyCatContext context) {
    super.init(context);

    // Set the current leader as this replica.
    context.setCurrentLeader(context.cluster.localMember().uri());

    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    setPingTimer();

    // When the leader is first elected, it needs to commit any pending commands
    // in its log to the state machine and then commit a snapshot to its log.
    // This methodology differs slightly from the standard Raft algorithm. Instead
    // if storing snapshots in a separate file, we store them as normal log entries.
    // Using this methodology, *all* nodes should always have a snapshot as the
    // first entry in their log, whether it be a local snapshot or a snapshot that
    // was replicated by the leader. This greatly simplifies snapshot management as
    // snapshots are simply replicated as a normal part of each node's log.
    for (long i = context.getLastApplied(); i < context.log.lastIndex(); i++) {
      applyEntry(i);
    }

    // Once all pending entries have been applied to the state machine, create
    // a new snapshot of the local state machine state, commit it to the local
    // log, and wipe the log of committed entries.
    Entries<SnapshotEntry> snapshotEntries = createSnapshot();
    long index = context.log.lastIndex() + 1;
    context.log.appendEntries(snapshotEntries);
    context.log.removeBefore(index);

    // Next, the leader must write a no-op entry to the log and replicate the log
    // to all the nodes in the cluster. This ensures that other nodes are notified
    // of the leader's election and that their terms are updated with the leader's term.
    context.log.appendEntry(new NoOpEntry(context.getCurrentTerm()));

    // Ensure that the cluster configuration is up-to-date and properly
    // replicated by committing the current configuration to the log. This will
    // ensure that nodes' cluster configurations are consistent with the leader's.
    Set<String> members = new HashSet<>(context.cluster.config().getMembers());
    context.log.appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members));

    // Start observing the user provided cluster configuration for changes.
    // When the cluster configuration changes, changes will be committed to the
    // log and replicated according to the Raft specification.
    context.cluster.config().addObserver(this);

    // Create a map and list of remote replicas. We create both a map and
    // list because the list is sortable, so we can use a little math
    // trick to determine the current cluster-wide log commit index.
    for (String uri : context.cluster.config().getRemoteMembers()) {
      if (!replicaMap.containsKey(uri)) {
        Member member = context.cluster.member(uri);
        if (member != null) {
          RemoteReplica replica = new RemoteReplica(member);
          replicaMap.put(uri, replica);
          replica.open();
          replicas.add(replica);
        }
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
    // configuration if it is indeed observable. We have to be very careful about
    // the order in which cluster configuration changes occur. If two configuration
    // changes are taking place at the same time, one can overwrite the other.
    // Additionally, if a new cluster configuration immediately overwrites an old
    // configuration without first replicating a combined old/new configuration,
    // a dual-majority can result, meaning logs will ultimately become out of sync.
    // In order to avoid this, we need to perform a two-step configuration change:
    // - First log the combined current cluster configuration and new cluster
    // configuration. For instance, if a node was added to the cluster, log the
    // new configuration. If a node was removed, log the old configuration.
    // - Once the combined cluster configuration has been replicated, log and
    // sync the new configuration.
    // This two-step process ensures log consistency by ensuring that two majorities
    // cannot result from adding and removing too many nodes at once.

    // First, store the set of new members in a final local variable. We don't want
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
        combinedRemoteMembers.remove(context.cluster.localMember().uri());
        context.cluster.config().setRemoteMembers(combinedMembers);

        // Whenever the local cluster membership changes, ensure we update the
        // local replicas.
        for (String uri : combinedRemoteMembers) {
          if (!replicaMap.containsKey(uri)) {
            Member member = context.cluster.member(uri);
            if (member != null) {
              RemoteReplica replica = new RemoteReplica(member);
              replicaMap.put(uri, replica);
              replicas.add(replica);
              replica.open();
              replica.update();
            }
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
            remoteMembers.remove(context.cluster.localMember().uri());
            context.cluster.config().setRemoteMembers(remoteMembers);

            // Once the local internal configuration has been updated, remove
            // any replicas that are no longer in the cluster.
            Iterator<RemoteReplica> iterator = replicas.iterator();
            while (iterator.hasNext()) {
              RemoteReplica replica = iterator.next();
              if (!remoteMembers.contains(replica.member.uri())) {
                replica.close();
                iterator.remove();
                replicaMap.remove(replica.member.uri());
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
    currentTimer = context.config().getTimerStrategy().schedule(() -> {
      for (RemoteReplica replica : replicas) {
        replica.update();
      }
      setPingTimer();
    }, context.config().getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      return super.appendEntries(request);
    } else if (request.term() < context.getCurrentTerm()) {
      return CompletableFuture.completedFuture(new AppendEntriesResponse(request.id(), context.getCurrentTerm(), false));
    } else {
      context.transition(Follower.class);
      return super.appendEntries(request);
    }
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(final SubmitCommandRequest request) {
    CompletableFuture<SubmitCommandResponse> future = new CompletableFuture<>();

    // Determine the type of command this request is executing. The command
    // type is provided by a CommandProvider which provides CommandInfo for a
    // given command. If no CommandInfo is provided then all commands are assumed
    // to be READ_WRITE commands.
    Command command = context.stateMachineExecutor.getCommand(request.command());

    // Depending on the command type, read or write commands may or may not be replicated
    // to a quorum based on configuration  options. For write commands, if a quorum is
    // required then the command will be replicated. For read commands, if a quorum is
    // required then we simply ping a quorum of the cluster to ensure that data is not stale.
    if (command != null && command.type().equals(Command.Type.READ)) {
      // Users have the option of whether to allow stale data to be returned. By
      // default, read quorums are enabled. If read quorums are disabled then we
      // simply apply the command, otherwise we need to ping a quorum of the
      // cluster to ensure that data is up-to-date before responding.
      if (context.config().isRequireReadQuorum()) {
        final Quorum quorum = new Quorum(context.cluster.config().getQuorumSize(), (succeeded) -> {
          if (succeeded) {
            try {
              future.complete(new SubmitCommandResponse(request.id(), context.stateMachineExecutor.applyCommand(request.command(), request.args())));
            } catch (Exception e) {
              future.completeExceptionally(e);
            }
          } else {
            future.completeExceptionally(new CopyCatException("Failed to acquire read quorum"));
          }
        });

        // To acquire a read quorum, iterate through all the available remote
        // replicas and ping each replica. This will ensure that remote replicas
        // are still up-to-date with the leader's log.
        for (RemoteReplica replica : replicas) {
          replica.ping().whenComplete((result, error) -> {
            if (error != null) {
              quorum.fail();
            } else {
              quorum.succeed();
            }
          });
        }
      } else {
        try {
          future.complete(new SubmitCommandResponse(request.id(), context.stateMachineExecutor.applyCommand(request.command(), request.args())));
        } catch (Exception e) {
          future.completeExceptionally(e);
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
              future.complete(new SubmitCommandResponse(request.id(), context.stateMachineExecutor.applyCommand(request.command(), request.args())));
            } catch (Exception e) {
              future.completeExceptionally(e);
            } finally {
              context.setLastApplied(index);
              compactLog();
            }
          }
        });
      } else {
        // If write quorums are not required then just apply it and return the
        // result. We don't need to check the order of the application here since
        // all entries written to the log will not require a quorum and thus
        // we won't be applying any entries out of order.
        try {
          future.complete(new SubmitCommandResponse(request.id(), context.stateMachineExecutor.applyCommand(request.command(), request.args())));
        } catch (Exception e) {
          future.completeExceptionally(e);
        } finally {
          context.setLastApplied(index);
          compactLog();
        }
      }
    }
    return future;
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
        iterator.remove();
        entry.getValue().run();
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
      int index = replicas.size() + 1 - context.cluster.config().getQuorumSize();
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
    if (currentTimer != null) {
      currentTimer.cancel(true);
    }
    // Stop observing the observable cluster configuration.
    context.cluster.config().deleteObserver(this);
  }

  /**
   * Represents a reference to a remote replica.
   */
  private class RemoteReplica {
    private final Member member;
    private boolean open;
    private long nextIndex;
    private long matchIndex;
    private AtomicBoolean running = new AtomicBoolean();
    private final List<CompletableFuture<Void>> responseFutures = new CopyOnWriteArrayList<>();

    public RemoteReplica(Member member) {
      this.member = member;
      this.nextIndex = context.log.lastIndex();
    }

    /**
     * Opens the connection to the replica.
     */
    private void open() {
      if (open == false && running.compareAndSet(false, true)) {
        member.protocol().client().connect().whenComplete((result, error) -> {
          if (error != null) {
            running.set(false);
            triggerFutures(error);
          } else {
            open = true;
            running.set(false);
            triggerFutures();
          }
        });
      }
    }

    /**
     * Updates the replica, either synchronizing new log entries to the replica
     * or by pinging the replica.
     */
    private void update() {
      if (open && running.compareAndSet(false, true)) {
        if (nextIndex <= context.log.lastIndex() || matchIndex < context.getCommitIndex()) {
          doSync();
        } else {
          doPing();
        }
      }
    }

    /**
     * Begins synchronization with the replica.
     */
    private void doSync() {
      final long prevIndex = nextIndex - 1;
      final Entry prevEntry = context.log.getEntry(prevIndex);

      // Create a list of up to ten entries to send to the follower.
      // We can only send one snapshot entry in any given request. So, if any of
      // the entries are snapshot entries, send all entries up to the snapshot and
      // then send snapshot entries individually.
      List<Entry> entries = new ArrayList<>();
      long lastIndex = nextIndex + 10 > context.log.lastIndex() ? context.log.lastIndex() : nextIndex + 10;
      for (long i = nextIndex; i <= lastIndex; i++) {
        Entry entry = context.log.getEntry(i);

        // If we ran into a snapshot entry, immediately send all entries up to the
        // snapshot entry. If the snapshot entry is the first entry, send snapshot
        // entries one at a time.
        if (entry instanceof SnapshotEntry) {
          if (entries.isEmpty()) {
            doAppendEntries(prevIndex, prevEntry, Arrays.asList(entry));
          } else {
            doAppendEntries(prevIndex, prevEntry, entries);
          }
          return;
        } else {
          entries.add(entry);
        }
      }

      doAppendEntries(prevIndex, prevEntry, entries);
    }

    /**
     * Sends an append entries request to the follower.
     */
    private void doAppendEntries(long prevIndex, Entry prevEntry, List<Entry> entries) {
      final long commitIndex = context.getCommitIndex();
      if (logger.isLoggable(Level.FINER)) {
        if (!entries.isEmpty()) {
          if (entries.size() > 1) {
            logger.finer(String.format("%s replicating entries %d-%d to %s", context.cluster.localMember(), prevIndex+1, prevIndex+entries.size(), member.uri()));
          } else {
            logger.finer(String.format("%s replicating entry %d to %s", context.cluster.localMember(), prevIndex+1, member.uri()));
          }
        } else {
          logger.finer(String.format("%s committing entry %d to %s", context.cluster.localMember(), commitIndex, member.uri()));
        }
      }

      AppendEntriesRequest request = new AppendEntriesRequest(context.nextCorrelationId(), context.getCurrentTerm(), context.cluster.localMember().uri(), prevIndex, prevEntry != null ? prevEntry.term() : 0, entries, commitIndex);
      member.protocol().client().appendEntries(request).whenCompleteAsync((response, error) -> {
        if (error != null) {
          running.set(false);
          logger.warning(error.getMessage());
          triggerFutures(error);
        } else {
          if (response.status().equals(Response.Status.OK)) {
            if (response.succeeded()) {
              if (logger.isLoggable(Level.FINER)) {
                if (!entries.isEmpty()) {
                  if (entries.size() > 1) {
                    logger.finer(String.format("%s successfully replicated entries %d-%d to %s", context.cluster.localMember(), prevIndex+1, prevIndex+entries.size(), member.uri()));
                  } else {
                    logger.finer(String.format("%s successfully replicated entry %d to %s", context.cluster.localMember(), prevIndex+1, member.uri()));
                  }
                } else {
                  logger.finer(String.format("%s successfully committed entry %d to %s", context.cluster.localMember(), commitIndex, member.uri()));
                }
              }

              // Update the next index to send and the last index known to be replicated.
              if (!entries.isEmpty()) {
                nextIndex = Math.max(nextIndex + 1, prevIndex + entries.size() + 1);
                matchIndex = Math.max(matchIndex, prevIndex + entries.size());
                checkCommits();
                doSync();
              } else {
                running.set(false);
              }
            } else {
              if (logger.isLoggable(Level.FINER)) {
                if (!entries.isEmpty()) {
                  if (entries.size() > 1) {
                    logger.finer(String.format("%s failed to replicate entries %d-%d to %s", context.cluster.localMember(), prevIndex+1, prevIndex+entries.size(), member.uri()));
                  } else {
                    logger.finer(String.format("%s failed to replicate entry %d to %s", context.cluster.localMember(), prevIndex+1, member.uri()));
                  }
                } else {
                  logger.finer(String.format("%s failed to commit entry %d to %s", context.cluster.localMember(), commitIndex, member.uri()));
                }
              }

              // If replication failed then decrement the next index and attemt to
              // retry replication. If decrementing the next index would result in
              // a next index of 0 then something must have gone wrong. Revert to
              // a follower.
              if (nextIndex-1 == 0) {
                running.set(false);
                context.transition(Follower.class);
              } else {
                // If we were attempting to replicate log entries and not just
                // sending a commit index or if we didn't have any log entries
                // to replicate then decrement the next index. The node we were
                // attempting to sync is not up to date.
                if (!entries.isEmpty() || prevIndex == commitIndex) {
                  nextIndex--;
                  checkCommits();
                  doSync();
                } else {
                  checkCommits();
                  running.set(false);
                }
              }
            }
          } else {
            running.set(false);
            logger.warning(response.error().getMessage());
            triggerFutures(response.error());
          }
        }
      });
    }

    /**
     * Pings the replica.
     */
    private CompletableFuture<Void> ping() {
      if (open) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        responseFutures.add(future);
        if (running.compareAndSet(false, true)) {
          doPing();
        }
      }
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Pins the replica.
     */
    private void doPing() {
      // The "ping" request is simply an empty synchronization request.
      AppendEntriesRequest request = new AppendEntriesRequest(context.nextCorrelationId(), context.getCurrentTerm(), context.cluster.localMember().uri(), nextIndex-1, context.log.containsEntry(nextIndex-1) ? context.log.getEntry(nextIndex-1).term() : 0, new ArrayList<Entry>(), context.getCommitIndex());
      member.protocol().client().appendEntries(request).whenCompleteAsync((response, error) -> {
        if (error != null) {
          running.set(false);
          triggerFutures(error);
        } else {
          running.set(false);
          if (response.status().equals(Response.Status.OK)) {
            if (response.term() > context.getCurrentTerm()) {
              context.setCurrentTerm(response.term());
              context.setCurrentLeader(null);
              context.transition(Follower.class);
              triggerFutures(new CopyCatException("Not the leader"));
            } else {
              triggerFutures();
            }
          } else {
            triggerFutures(new CopyCatException(response.error()));
          }
          checkCommits();
        }
      });
    }

    /**
     * Triggers response futures with a completion result.
     */
    private void triggerFutures() {
      for (CompletableFuture<Void> future : responseFutures) {
        future.complete(null);
      }
      responseFutures.clear();
    }

    /**
     * Triggers response futures with an error result.
     */
    private void triggerFutures(Throwable t) {
      for (CompletableFuture<Void> future : responseFutures) {
        future.completeExceptionally(t);
      }
      responseFutures.clear();
    }

    /**
     * Closes down the replica.
     */
    private void close() {
      if (open) {
        open = false;
        member.protocol().client().close();
      }
    }
  }

}
