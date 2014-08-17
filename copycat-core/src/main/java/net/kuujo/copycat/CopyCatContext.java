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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DefaultCluster;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.MemoryLog;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;


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
  private final Registry registry;
  final Cluster cluster;
  final Log log;
  final StateMachineExecutor stateMachineExecutor;
  private final StateMachine stateMachine;
  private State state;
  private CompletableFuture<String> startFuture;
  private CopyCatConfig config;
  private String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>();
  private boolean leaderConnected;
  private long currentTerm;
  private String lastVotedFor;
  private long commitIndex = 0;
  private long lastApplied = 0;

  public CopyCatContext(StateMachine stateMachine) {
    this(stateMachine, new MemoryLog(), new ClusterConfig(), new CopyCatConfig());
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
    this(stateMachine, log, new ClusterConfig(), new CopyCatConfig());
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
    this.cluster = new DefaultCluster(cluster, this);
    this.stateMachine = stateMachine;
    this.stateMachineExecutor = new StateMachineExecutor(stateMachine);
  }

  /**
   * Kills the copycat instance.
   */
  void kill() {
    transition(None.class);
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
   * Returns the internal CopyCat cluster. Note that this cluster's configuration
   * may differ from the configuration passed by the user.
   *
   * @return The internal CopyCat cluster.
   */
  public Cluster cluster() {
    return cluster;
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
   * @return A completable future to be completed once the context has started.
   */
  public CompletableFuture<String> start() {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    CompletableFuture<String> future = new CompletableFuture<>();
    transition(None.class);
    cluster.localMember().protocol().server().start().whenCompleteAsync((result, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        startFuture = future;
        transition(Follower.class);
      }
    });
    return future;
  }

  /**
   * Checks whether the start handler needs to be called.
   */
  private void checkStart() {
    if (currentLeader != null && startFuture != null) {
      startFuture.complete(currentLeader);
      startFuture = null;
    }
  }

  /**
   * Stops the context.
   *
   * @return A completable future that will be completed when the context has started.
   */
  public CompletableFuture<Void> stop() {
    return cluster.localMember().protocol().server().stop().thenRunAsync(() -> {
      log.close();
      transition(None.class);
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
    logger.info(cluster.localMember() + " transitioning to " + type.toString());
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
    return currentLeader != null && currentLeader.equals(cluster.localMember());
  }


  String getCurrentLeader() {
    return currentLeader;
  }

  CopyCatContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
    }
    currentLeader = leader;

    // When a new leader is elected, we create a client and connect to the leader.
    // Once this node is connected to the leader we can begin submitting commands.
    if (leader == null) {
      leaderConnected = false;
      leaderClient = null;
    } else if (!isLeader()) {
      leaderConnected = false;
      leaderClient = cluster.member(currentLeader).protocol().client();
      leaderClient.connect().thenRun(() -> {
        leaderConnected = true;
        Iterator<Runnable> iterator = leaderConnectCallbacks.iterator();
        while (iterator.hasNext()) {
          Runnable runnable = iterator.next();
          iterator.remove();
          runnable.run();
        }
        checkStart();
      });
    } else {
      leaderConnected = true;
      Iterator<Runnable> iterator = leaderConnectCallbacks.iterator();
      while (iterator.hasNext()) {
        Runnable runnable = iterator.next();
        iterator.remove();
        runnable.run();
      }
      checkStart();
    }
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

  Object nextCorrelationId() {
    return config.getCorrelationStrategy().nextCorrelationId(this);
  }

  /**
   * Submits a command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param args An ordered list of command arguments.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submitCommand(final String command, final Object... args) {
    CompletableFuture<R> future = new CompletableFuture<>();
    if (currentLeader == null) {
      future.completeExceptionally(new CopyCatException("No leader available"));
    } else if (!leaderConnected) {
      leaderConnectCallbacks.add(() -> {
        leaderClient.submitCommand(new SubmitCommandRequest(nextCorrelationId(), command, Arrays.asList(args))).whenComplete((result, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
          } else {
            if (result.status().equals(Response.Status.OK)) {
              future.complete((R) result.result());
            } else {
              future.completeExceptionally(result.error());
            }
          }
        });
      });
    } else {
      ProtocolHandler handler = currentLeader.equals(cluster.localMember()) ? state : leaderClient;
      handler.submitCommand(new SubmitCommandRequest(nextCorrelationId(), command, Arrays.asList(args))).whenComplete((result, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          if (result.status().equals(Response.Status.OK)) {
            future.complete((R) result.result());
          } else {
            future.completeExceptionally(result.error());
          }
        }
      });
    }
    return future;
  }

}
