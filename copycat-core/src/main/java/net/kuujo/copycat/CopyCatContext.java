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

import java.util.logging.Logger;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DefaultCluster;
import net.kuujo.copycat.cluster.impl.DynamicClusterConfig;
import net.kuujo.copycat.cluster.impl.StaticClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.MemoryLog;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * CopyCat replica context.<p>
 *
 * The <code>CopyCatContext</code> is the primary API for creating
 * and running a CopyCat replica. Given a state machine, a cluster
 * configuration, and a log, the context will communicate with other
 * nodes in the cluster, applying and replicating state machine commands.<p>
 *
 * CopyCat uses a Raft-based consensus algorithm to perform leader election
 * and state machine replication. In CopyCat, all state changes are made
 * through the cluster leader. When a cluster is started, nodes will
 * communicate with one another to elect a leader. When a command is submitted
 * to any node in the cluster, the command will be forwarded to the leader.
 * When the leader receives a command submission, it will first replicate
 * the command to its followers before applying the command to its state
 * machine and returning the result.<p>
 *
 * In order to prevent logs from growing too large, CopyCat uses snapshotting
 * to periodically compact logs. In CopyCat, snapshots are simply log
 * entries before which all previous entries are cleared. When a node first
 * becomes the cluster leader, it will first commit a snapshot of its current
 * state to its log. This snapshot can be used to get any new nodes up to date.<p>
 *
 * CopyCat supports dynamic cluster membership changes. If the {@link ClusterConfig}
 * provided to the CopyCat context is {@link java.util.Observable}, the cluster
 * leader will observe the configuration for changes. Note that cluster membership
 * changes can only occur on the leader's cluster configuration. This is because,
 * as with all state changes, cluster membership changes must go through the leader.
 * When cluster membership changes occur, the cluster leader will log and replicate
 * the configuration change just like any other state change, and it will ensure
 * that the membership change occurs in a manner that prevents a dual-majority
 * in the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatContext {
  private static final Logger logger = Logger.getLogger(CopyCatContext.class.getCanonicalName());
  private final ClusterConfig clusterConfig;
  private final Registry registry;
  private final DynamicClusterConfig internalConfig = new DynamicClusterConfig();
  final Cluster cluster;
  final Log log;
  final StateMachine stateMachine;
  private State state;
  private AsyncCallback<String> startCallback;
  private CopyCatConfig config;
  private String currentLeader;
  private long currentTerm;
  private String lastVotedFor;
  private long commitIndex = 0;
  private long lastApplied = 0;

  public CopyCatContext(StateMachine stateMachine) {
    this(stateMachine, new MemoryLog(), new StaticClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster) {
    this(stateMachine, new MemoryLog(), cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster, Registry registry) {
    this(stateMachine, new MemoryLog(), cluster, new CopyCatConfig(), registry);
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this(stateMachine, new MemoryLog(), cluster, config, registry);
  }

  public CopyCatContext(StateMachine stateMachine, Log log) {
    this(stateMachine, log, new StaticClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster) {
    this(stateMachine, log, cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config) {
    this(stateMachine, log, cluster, config, new ConcurrentRegistry());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.log = log;
    this.config = config;
    this.registry = registry;
    this.clusterConfig = cluster;
    this.cluster = new DefaultCluster(cluster, this);
    this.stateMachine = stateMachine;
  }

  /**
   * Returns the replica configuration.
   *
   * @return The replica configuration.
   */
  public CopyCatConfig config() {
    return config;
  }

  /**
   * Returns the user-defined cluster configuration.<p>
   *
   * Note that because of the nature of CopyCat's dynamic cluster membership
   * support, the user-defined cluster configuration may temporarily differ
   * from the internal CopyCat cluster configuration. This is because when
   * the cluster configuration changes, CopyCat needs to replicate the configuration
   * change before officially committing the change in order to ensure log consistency.
   *
   * @return The user-defined cluster configuration.
   */
  public ClusterConfig cluster() {
    return clusterConfig;
  }

  /**
   * Returns the underlying state machine.
   *
   * @return The underlying state machine.
   */
  public StateMachine stateMachine() {
    return stateMachine;
  }

  /**
   * Returns the local log.
   *
   * @return The local log.
   */
  public Log log() {
    return log;
  }

  /**
   * Returns the context registry.<p>
   *
   * The registry can be used to register objects that can be accessed
   * by {@link net.kuujo.copycat.protocol.Protocol} and
   * {@link net.kuujo.copycat.endpoint.Endpoint} implementations.
   *
   * @return The context registry.
   */
  public Registry registry() {
    return registry;
  }

  /**
   * Starts the context.
   *
   * @return The replica context.
   */
  public CopyCatContext start() {
    return start(null);
  }

  /**
   * Starts the context.
   *
   * @param doneHandler An asynchronous handler to be called once the context
   *        has been started.
   * @return The replica context.
   */
  public CopyCatContext start(final AsyncCallback<String> callback) {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    internalConfig.setLocalMember(clusterConfig.getLocalMember());
    internalConfig.setRemoteMembers(clusterConfig.getRemoteMembers());

    transition(None.class);
    cluster.localMember().protocol().server().start(new AsyncCallback<Void>() {
      @Override
      public void complete(Void value) {
        startCallback = callback;
        transition(Follower.class);
      }
      @Override
      public void fail(Throwable t) {
        callback.fail(t);
      }
    });
    return this;
  }

  /**
   * Checks whether the start handler needs to be called.
   */
  private void checkStart() {
    if (currentLeader != null && startCallback != null) {
      startCallback.complete(currentLeader);
      startCallback = null;
    }
  }

  /**
   * Stops the context.
   *
   * @return The replica context.
   */
  public void stop() {
    stop(null);
  }

  /**
   * Stops the context.
   *
   * @param callback An asynchronous callback to be called once the context
   *        has been stopped.
   * @return The replica context.
   */
  public void stop(final AsyncCallback<Void> callback) {
    cluster.localMember().protocol().server().stop(new AsyncCallback<Void>() {
      @Override
      public void complete(Void value) {
        log.close();
        transition(None.class);
      }
      @Override
      public void fail(Throwable t) {
        logger.severe("Failed to stop context");
      }
    });
  }

  /**
   * Transitions the context to a new state.
   *
   * @param type The state to which to transition.
   */
  void transition(Class<? extends State> type) {
    if (this.state != null && type != null && type.isAssignableFrom(this.state.getClass()))
      return;
    logger.info(cluster.config().getLocalMember() + " transitioning to " + type.toString());
    final State oldState = state;
    try {
      state = type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      // Log the exception.
    }
    if (oldState != null) {
      oldState.destroy();
      state.init(this);
    } else {
      state.init(this);
    }
  }

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  public String leader() {
    return currentLeader;
  }

  /**
   * Returns a boolean indicating whether this node is the leader.
   *
   * @return Indicates whether this node is the leader.
   */
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(cluster.localMember().uri());
  }


  String getCurrentLeader() {
    return currentLeader;
  }

  CopyCatContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
    }
    currentLeader = leader;
    checkStart();
    return this;
  }

  long getCurrentTerm() {
    return currentTerm;
  }

  CopyCatContext setCurrentTerm(long term) {
    if (term > currentTerm) {
      currentTerm = term;
      logger.finer(String.format("Updated current term %d", term));
      lastVotedFor = null;
    }
    return this;
  }

  String getLastVotedFor() {
    return lastVotedFor;
  }

  CopyCatContext setLastVotedFor(String candidate) {
    if (lastVotedFor == null || !lastVotedFor.equals(candidate)) {
      logger.finer(String.format("Voted for %s", candidate));
    }
    lastVotedFor = candidate;
    return this;
  }

  long getCommitIndex() {
    return commitIndex;
  }

  CopyCatContext setCommitIndex(long index) {
    commitIndex = Math.max(commitIndex, index);
    return this;
  }

  long getLastApplied() {
    return lastApplied;
  }

  CopyCatContext setLastApplied(long index) {
    lastApplied = index;
    return this;
  }

  /**
   * Submits a command to the service.
   *
   * @param command The command to submit.
   * @param args Command arguments.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The replica context.
   */
  public CopyCatContext submitCommand(final String command, Arguments args, final AsyncCallback<Object> callback) {
    if (currentLeader == null) {
      callback.fail(new CopyCatException("No leader available"));
    } else {
      cluster.member(currentLeader).protocol().client().submitCommand(new SubmitCommandRequest(command, args), new AsyncCallback<SubmitCommandResponse>() {
        @Override
        public void complete(SubmitCommandResponse response) {
          if (response.status().equals(Response.Status.OK)) {
            callback.complete(response.result());
          } else {
            callback.fail(response.error());
          }
        }
        @Override
        public void fail(Throwable t) {
          callback.fail(t);
        }
      });
    }
    return this;
  }

}
