/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.state;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.event.MembershipChangeEvent;
import net.kuujo.copycat.internal.StateMachineExecutor;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.replication.ClusterReplicator;
import net.kuujo.copycat.internal.replication.Replicator;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader state.<p>
 *
 * The leader state is assigned to replicas who have assumed
 * a leadership role in the cluster through a cluster-wide election.
 * The leader's role is to receive operation submissions and log and
 * replicate state changes. All state changes go through the leader
 * for simplicity.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderController extends StateController implements Observer {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderController.class);
  private ScheduledFuture<Void> currentTimer;
  private Replicator replicator;

  @Override
  CopycatState state() {
    return CopycatState.LEADER;
  }

  @Override
  Logger logger() {
    return LOGGER;
  }

  @Override
  public void init(StateContext context) {
    super.init(context);

    replicator = new ClusterReplicator(context);

    // When the leader is first elected, it needs to commit any pending operations
    // in its log to the state machine and then commit a snapshot to its log.
    // This methodology differs slightly from the standard Raft algorithm. Instead
    // if storing snapshots in a separate file, we store them as normal log entries.
    // Using this methodology, *all* nodes should always have a snapshot as the
    // first entry in their log, whether it be a local snapshot or a snapshot that
    // was replicated by the leader. This greatly simplifies snapshot management as
    // snapshots are simply replicated as a normal part of each node's log.
    int count = 0;
    for (long i = context.lastApplied() + 1; i <= context.log().lastIndex(); i++) {
      applyEntry(i);
      count++;
    }
    LOGGER.debug("{} - Applied {} entries to state machine", context.clusterManager().localNode(), count);

    // Ensure that the cluster configuration is up-to-date and properly
    // replicated by committing the current configuration to the log. This will
    // ensure that nodes' cluster configurations are consistent with the leader's.
    ConfigurationEntry configEntry = new ConfigurationEntry(context.currentTerm(), context.clusterManager().cluster().copy());
    long configIndex = context.log().appendEntry(configEntry);
    LOGGER.debug("{} - Appended {} to log at index {}", context.clusterManager().localNode(), configEntry, configIndex);

    // Start observing the user provided cluster configuration for changes.
    // When the cluster configuration changes, changes will be committed to the
    // log and replicated according to the Raft specification.
    context.cluster().addObserver(this);
    LOGGER.debug("{} - Observing {}", context.clusterManager().localNode(), context.cluster());

    // Set the current leader as this replica.
    context.currentLeader(context.clusterManager().localNode().member().id());

    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
   replicator.pingAll();

    LOGGER.debug("{} - Setting ping timer", context.clusterManager().localNode());
    setPingTimer();
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged((Cluster) o);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private synchronized void clusterChanged(final Cluster cluster) {
    // All cluster configuration changes must go through the leader. In order to
    // perform cluster configuration changes, the leader observes the local cluster
    // configuration if it is indeed observable. We have to be very careful about
    // the order in which cluster configuration changes occur. If two configuration
    // changes are taking place at the same time, one can overwrite the other.
    // Additionally, if a new cluster configuration immediately overwrites an old
    // configuration without first replicating a joint old/new configuration,
    // a dual-majority can result, meaning logs will ultimately become out of sync.
    // In order to avoid this, we need to perform a two-step configuration change:
    // - First log the combined current cluster configuration and new cluster
    // configuration. For instance, if a node was added to the cluster, log the
    // new configuration. If a node was removed, log the old configuration.
    // - Once the joint cluster configuration has been replicated, log and
    // sync the new configuration.
    // This two-step process ensures log consistency by ensuring that two majorities
    // cannot result from adding and removing too many nodes at once.
    LOGGER.debug("{} - Detected configuration change {}", context.clusterManager().localNode(), cluster);

    // First, store a copy of both the current internal cluster configuration and
    // the user defined cluster configuration. This ensures that mutable configurations
    // are not changed during the reconfiguration process which can be asynchronous.
    // Note also that we create a copy of the configuration in order to ensure that
    // polymorphic types are properly reconstructed.
    final Cluster userCluster = cluster.copy();
    final Cluster internalCluster = context.clusterManager().cluster().copy();

    // If another cluster configuration change is occurring right now, it's possible
    // that the two configuration changes could overlap one another. In order to
    // avoid this, we wait until all entries up to the current log index have been
    // committed before beginning the configuration change. This ensures that any
    // previous configuration changes have completed.
    LOGGER.debug("{} - Committing all entries for configuration change", context.clusterManager().localNode());
    replicator.commitAll().whenComplete((commitIndex, commitError) -> {
      // First we need to create a joint old/new cluster configuration entry.
      // We copy the internal configuration again for safety from modifications.
      final Cluster jointCluster = internalCluster.copy();
      jointCluster.addRemoteMembers(userCluster.remoteEndpoints());

      // Append the joint configuration to the log. This will be replicated to
      // followers and applied to their internal cluster managers.
      ConfigurationEntry jointConfigEntry = new ConfigurationEntry(context.currentTerm(), jointCluster);
      long configIndex = context.log().appendEntry(jointConfigEntry);
      LOGGER.debug("{} - Appended {} to log at index {}", context.clusterManager().localNode(), jointConfigEntry, configIndex);

      // Immediately after the entry is appended to the log, apply the joint
      // configuration. Cluster membership changes do not wait for commitment.
      // Since we're using a joint consensus, it's safe to work with all members
      // of both the old and new configuration without causing split elections.
      context.clusterManager().cluster().syncWith(jointCluster);
      context.events().membershipChange().handle(new MembershipChangeEvent(jointCluster));
      LOGGER.debug("{} - Updated internal cluster configuration {}", context.clusterManager().localNode(), context.clusterManager().cluster());

      // Once the cluster is updated, the replicator will be notified and update its
      // internal connections. Then we commit the joint configuration and allow
      // it to be replicated to all the nodes in the updated cluster.
      LOGGER.debug("{} - Committing all entries for configuration change", context.clusterManager().localNode());
      replicator.commit(configIndex).whenComplete((commitIndex2, commitError2) -> {
        // Now that we've gotten to this point, we know that the combined cluster
        // membership has been replicated to a majority of the cluster.
        // Append the new user configuration to the log and force all replicas
        // to be synchronized.
        ConfigurationEntry newConfigEntry = new ConfigurationEntry(context.currentTerm(), userCluster);
        long newConfigIndex = context.log().appendEntry(newConfigEntry);
        LOGGER.debug("{} - Appended {} to log at index {}", context.clusterManager().localNode(), newConfigEntry, newConfigIndex);

        // Again, once we've appended the new configuration to the log, update
        // the local internal configuration.
        context.clusterManager().cluster().syncWith(userCluster);
        context.events().membershipChange().handle(new MembershipChangeEvent(userCluster));
        LOGGER.debug("{} - Updated internal cluster configuration {}", context.clusterManager().localNode(), context.clusterManager().cluster());

        // Note again that when the cluster membership changes, the replicator will
        // be notified and remove any replicas that are no longer a part of the cluster.
        // Now that the cluster and replicator have been updated, we can commit the
        // new configuration.
        LOGGER.debug("{} - Committing all entries for configuration change", context.clusterManager().localNode());
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
  public CompletableFuture<PingResponse> ping(final PingRequest request) {
    if (request.term() > context.currentTerm()) {
      return super.ping(request);
    } else if (request.term() < context.currentTerm()) {
      return CompletableFuture.completedFuture(logResponse(new PingResponse(logRequest(request).id(), context.currentTerm(), false)));
    } else {
      context.transition(FollowerController.class);
      return super.ping(request);
    }
  }

  @Override
  public CompletableFuture<SyncResponse> sync(final SyncRequest request) {
    if (request.term() > context.currentTerm()) {
      return super.sync(request);
    } else if (request.term() < context.currentTerm()) {
      return CompletableFuture.completedFuture(logResponse(new SyncResponse(logRequest(request).id(), context.currentTerm(), false, context.log().lastIndex())));
    } else {
      context.transition(FollowerController.class);
      return super.sync(request);
    }
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(final SubmitRequest request) {
    logRequest(request);

    CompletableFuture<SubmitResponse> future = new CompletableFuture<>();

    // Find the named operation.
    StateMachineExecutor.Operation operation = context.stateMachineExecutor().getOperation(request.operation());

    // If the operation is unknown the immediately return a failure.
    if (operation == null) {
      future.completeExceptionally(new CopycatException("Invalid operation"));
    }
    // Depending on the operation type, read or write operations may or may not be replicated
    // to a quorum based on configuration options. For write operations, if a quorum is
    // required then the operation will be replicated. For read operations, if a quorum is
    // required then we simply ping a quorum of the cluster to ensure that data is not stale.
    else if (operation.isReadOnly()) {
      // Users have the option of whether to allow stale data to be returned. By
      // default, read quorums are enabled. If read quorums are disabled then we
      // simply apply the operation, otherwise we need to ping a quorum of the
      // cluster to ensure that data is up-to-date before responding.
      if (context.config().isRequireQueryQuorum()) {
        long lastIndex = context.log().lastIndex();
        LOGGER.debug("{} - Synchronizing logs to index {} for read", context.clusterManager().localNode(), lastIndex);
        replicator.ping(lastIndex).whenComplete((index, error) -> {
          if (error == null) {
            try {
              future.complete(logResponse(new SubmitResponse(request.id(), operation.apply(request.args()))));
            } catch (Exception e) {
              future.completeExceptionally(e);
            }
          } else {
            future.completeExceptionally(error);
          }
        });
      } else {
        try {
          future.complete(logResponse(new SubmitResponse(request.id(), operation.apply(request.args()))));
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      }
    } else {
      // For write operations or for operations for which the type is not known, an
      // entry must be logged, replicated, and committed prior to applying it
      // to the state machine and returning the result.
      OperationEntry entry = new OperationEntry(context.currentTerm(), request.operation(), request.args());
      final long index = context.log().appendEntry(entry);
      LOGGER.debug("{} - Appended {} to log at index {}", context.clusterManager().localNode(), entry, index);

      // Write quorums are also optional to the user. The user can optionally
      // indicate that write operations should be immediately applied to the state
      // machine and the result returned.
      if (context.config().isRequireCommandQuorum()) {
        // If the replica requires write quorums, we simply set a task to be
        // executed once the entry has been replicated to a quorum of the cluster.
        LOGGER.debug("{} - Replicating logs up to index {} for write", context.clusterManager().localNode(), index);
        replicator.commit(index).whenComplete((resultIndex, error) -> {
          if (error == null) {
            try {
              future.complete(logResponse(new SubmitResponse(request.id(), operation.apply(request.args()))));
            } catch (Exception e) {
              future.completeExceptionally(e);
            } finally {
              context.lastApplied(index);
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
          future.complete(logResponse(new SubmitResponse(request.id(), operation.apply(request.args()))));
        } catch (Exception e) {
          future.completeExceptionally(e);
        } finally {
          context.lastApplied(index);
          compactLog();
        }
      }
    }
    return future;
  }

  @Override
  void destroy() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling ping timer", context.clusterManager().localNode());
      currentTimer.cancel(true);
    }
    // Stop observing the observable cluster configuration.
    context.cluster().deleteObserver(this);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof LeaderController && ((StateController) object).context.equals(context);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + context.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("LeaderController[context=%s]", context);
  }

}
