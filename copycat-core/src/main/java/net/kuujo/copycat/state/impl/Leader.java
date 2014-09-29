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
package net.kuujo.copycat.state.impl;

import java.util.HashSet;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.NoOpEntry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.replication.Replicator;
import net.kuujo.copycat.replication.impl.RaftReplicator;
import net.kuujo.copycat.state.State;

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
public class Leader extends RaftState implements Observer {
  private ScheduledFuture<Void> currentTimer;
  private Replicator replicator;

  @Override
  public State.Type type() {
    return State.Type.LEADER;
  }

  @Override
  public void init(RaftStateContext context) {
    super.init(context);

    replicator = new RaftReplicator(context)
      .withReadQuorum(context.config().getReadQuorumSize())
      .withWriteQuorum(context.config().getWriteQuorumSize());

    // When the leader is first elected, it needs to commit any pending commands
    // in its log to the state machine and then commit a snapshot to its log.
    // This methodology differs slightly from the standard Raft algorithm. Instead
    // if storing snapshots in a separate file, we store them as normal log entries.
    // Using this methodology, *all* nodes should always have a snapshot as the
    // first entry in their log, whether it be a local snapshot or a snapshot that
    // was replicated by the leader. This greatly simplifies snapshot management as
    // snapshots are simply replicated as a normal part of each node's log.
    for (long i = context.getLastApplied() + 1; i <= context.log().lastIndex(); i++) {
      applyEntry(i);
    }

    // Next, the leader must write a no-op entry to the log and replicate the log
    // to all the nodes in the cluster. This ensures that other nodes are notified
    // of the leader's election and that their terms are updated with the leader's term.
    context.log().appendEntry(new NoOpEntry(context.getCurrentTerm()));

    // Ensure that the cluster configuration is up-to-date and properly
    // replicated by committing the current configuration to the log. This will
    // ensure that nodes' cluster configurations are consistent with the leader's.
    Set<String> members = new HashSet<>(context.clusterConfig().getMembers());
    context.log().appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members));

    // Start observing the user provided cluster configuration for changes.
    // When the cluster configuration changes, changes will be committed to the
    // log and replicated according to the Raft specification.
    context.cluster().config().addObserver(this);

    // Create a map and list of remote replicas. We create both a map and
    // list because the list is sortable, so we can use a little math
    // trick to determine the current cluster-wide log commit index.
    for (String uri : context.cluster().config().getRemoteMembers()) {
      Member member = context.cluster().member(uri);
      if (member != null && !replicator.containsMember(member)) {
        replicator.addMember(member);
      }
    }

    // Set the current leader as this replica.
    context.setCurrentLeader(context.clusterConfig().getLocalMember());

    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    setPingTimer();
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
    replicator.commitAll().whenComplete((commitIndex, commitError) -> {
      // First we need to commit a combined old/new cluster configuration entry.
      Set<String> combinedMembers = new HashSet<>(context.cluster().config().getMembers());
      combinedMembers.addAll(members);
      Entry entry = new ConfigurationEntry(context.getCurrentTerm(), combinedMembers);
      long configIndex = context.log().appendEntry(entry);

      // Immediately after the entry is appended to the log, apply the combined
      // membership. Cluster membership changes do not wait for commitment.
      Set<String> combinedRemoteMembers = new HashSet<>(combinedMembers);
      combinedRemoteMembers.remove(context.clusterConfig().getLocalMember());
      context.clusterConfig().setRemoteMembers(combinedMembers);

      // Whenever the local cluster membership changes, ensure we update the
      // local replicas.
      for (String uri : combinedRemoteMembers) {
        Member member = context.cluster().member(uri);
        if (member != null && !replicator.containsMember(member)) {
          replicator.addMember(member).commitAll();
        }
      }

      replicator.commit(configIndex).whenComplete((commitIndex2, commitError2) -> {
        // Append the new configuration entry to the log and force all replicas
        // to be synchronized.
        context.log().appendEntry(new ConfigurationEntry(context.getCurrentTerm(), members));

        // Again, once we've appended the new configuration to the log, update
        // the local internal configuration.
        Set<String> remoteMembers = new HashSet<>(members);
        remoteMembers.remove(context.clusterConfig().getLocalMember());
        context.clusterConfig().setRemoteMembers(remoteMembers);

        // Once the local internal configuration has been updated, remove
        // any replicas that are no longer in the cluster.
        for (Member member : replicator.getMembers()) {
          if (!remoteMembers.contains(member.uri())) {
            replicator.removeMember(member);
          }
        }

        // Force all replicas to be synchronized.
        replicator.commitAll();
      });
    });
  }

  /**
   * Resets the ping timer.
   */
  private void setPingTimer() {
    currentTimer = context.config().getTimerStrategy().schedule(() -> {
      replicator.pingAll();
      setPingTimer();
    }, context.config().getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      return super.appendEntries(request);
    } else if (request.term() < context.getCurrentTerm()) {
      return CompletableFuture.completedFuture(new AppendEntriesResponse(request.id(), context.getCurrentTerm(), false, context.log().lastIndex()));
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
    Command command = context.stateMachine().getCommand(request.command());

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
        replicator.ping(context.log().lastIndex()).whenComplete((index, error) -> {
          if (error == null) {
            try {
              future.complete(new SubmitCommandResponse(request.id(), context.stateMachine().applyCommand(request.command(), request.args())));
            } catch (Exception e) {
              future.completeExceptionally(e);
            }
          } else {
            future.completeExceptionally(error);
          }
        });
      } else {
        try {
          future.complete(new SubmitCommandResponse(request.id(), context.stateMachine().applyCommand(request.command(), request.args())));
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      }
    } else {
      // For write commands or for commands for which the type is not known, an
      // entry must be logged, replicated, and committed prior to applying it
      // to the state machine and returning the result.
      final long index = context.log().appendEntry(new CommandEntry(context.getCurrentTerm(), request.command(), request.args()));

      // Write quorums are also optional to the user. The user can optionally
      // indicate that write commands should be immediately applied to the state
      // machine and the result returned.
      if (context.config().isRequireWriteQuorum()) {
        // If the replica requires write quorums, we simply set a task to be
        // executed once the entry has been replicated to a quorum of the cluster.
        replicator.commit(index).whenComplete((resultIndex, error) -> {
          if (error == null) {
            try {
              future.complete(new SubmitCommandResponse(request.id(), context.stateMachine().applyCommand(request.command(), request.args())));
            } catch (Exception e) {
              future.completeExceptionally(e);
            } finally {
              context.setLastApplied(index);
              compactLog();
            }
          } else {
            future.completeExceptionally(error);
          }
        });
      } else {
        // If write quorums are not required then just apply it and return the
        // result. We don't need to check the order of the application here since
        // all entries written to the log will not require a quorum and thus
        // we won't be applying any entries out of order.
        try {
          future.complete(new SubmitCommandResponse(request.id(), context.stateMachine().applyCommand(request.command(), request.args())));
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

  @Override
  public void destroy() {
    if (currentTimer != null) {
      currentTimer.cancel(true);
    }
    // Stop observing the observable cluster configuration.
    context.cluster().config().deleteObserver(this);
  }

}
