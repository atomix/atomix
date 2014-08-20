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

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DefaultCluster;
import net.kuujo.copycat.election.ElectionContext;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogFactory;
import net.kuujo.copycat.log.impl.FileLogFactory;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;
import net.kuujo.copycat.state.StateContext;
import net.kuujo.copycat.state.impl.RaftStateContext;

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
  private final Registry registry;
  private final Cluster cluster;
  private final StateMachine stateMachine;
  private final ElectionContext election;
  private final RaftStateContext state;
  private final CopyCatConfig config;

  public CopyCatContext(StateMachine stateMachine) {
    this(stateMachine, new FileLogFactory(), new ClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster) {
    this(stateMachine, new FileLogFactory(), cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster, Registry registry) {
    this(stateMachine, new FileLogFactory(), cluster, new CopyCatConfig(), registry);
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this(stateMachine, new FileLogFactory(), cluster, config, registry);
  }

  public CopyCatContext(StateMachine stateMachine, LogFactory logFactory) {
    this(stateMachine, logFactory, new ClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster) {
    this(stateMachine, logFactory, cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config) {
    this(stateMachine, logFactory, cluster, config, new ConcurrentRegistry());
  }

  public CopyCatContext(StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.config = config;
    this.registry = registry;
    this.state = new RaftStateContext(this, logFactory);
    this.cluster = new DefaultCluster(cluster, state.cluster(), this);
    this.stateMachine = stateMachine;
    this.election = this.state.election();
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
   * Returns the underlying log.
   *
   * @return The underlying log.
   */
  public Log log() {
    return state.log();
  }

  /**
   * Returns the election context.
   *
   * @return The election context.
   */
  public ElectionContext election() {
    return election;
  }

  /**
   * Returns the state context.
   *
   * @return The state context.
   */
  public StateContext state() {
    return state;
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
  public CompletableFuture<Void> start() {
    return state.start();
  }

  /**
   * Stops the context.
   *
   * @return A completable future that will be completed when the context has started.
   */
  public CompletableFuture<Void> stop() {
    return state.stop();
  }

  /**
   * Submits a command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param args An ordered list of command arguments.
   * @return A completable future to be completed once the result is received.
   */
  public <R> CompletableFuture<R> submitCommand(final String command, final Object... args) {
    return state.submitCommand(command, args);
  }

}
