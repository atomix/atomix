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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.internal.StateMachineExecutor;
import net.kuujo.copycat.internal.cluster.ClusterManager;
import net.kuujo.copycat.internal.event.DefaultEventHandlers;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.spi.protocol.Protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class StateContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(StateContext.class);
  private final Cluster cluster;
  private final ClusterManager clusterManager;
  private final StateMachineExecutor stateMachineExecutor;
  private final Log log;
  private final CopycatConfig config;
  private final DefaultEventHandlers events = new DefaultEventHandlers();
  private volatile StateController currentState;
  private volatile String currentLeader;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  /**
   * @throws NullPointerException if any arguments are null
   */
  public StateContext(StateMachine stateMachine, Log log, Cluster cluster, Protocol protocol, CopycatConfig config) {
    this.stateMachineExecutor = new StateMachineExecutor(stateMachine);
    this.log = Assert.isNotNull(log, "log");
    this.cluster = Assert.isNotNull(cluster, "cluster");
    this.config = Assert.isNotNull(config, "config");
    this.clusterManager = new ClusterManager(cluster, protocol);
  }

  /**
   * Returns the state machine.
   *
   * @return The state machine.
   */
  public StateMachineExecutor stateMachineExecutor() {
    return stateMachineExecutor;
  }

  /**
   * Returns the copycat configuration.
   *
   * @return The copycat configuration.
   */
  public CopycatConfig config() {
    return config;
  }

  /**
   * Returns the copycat cluster.
   *
   * @return The copycat cluster.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the copycat cluster manager.
   *
   * @return The copycat cluster manager.
   */
  public ClusterManager clusterManager() {
    return clusterManager;
  }

  /**
   * Returns the copycat log.
   *
   * @return The copycat log.
   */
  public Log log() {
    return log;
  }

  /**
   * Returns the internal event registry.
   *
   * @return The internal event registry.
   */
  public DefaultEventHandlers events() {
    return events;
  }

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  public CopycatState state() {
    return currentState.state();
  }

  /**
   * Starts the state.
   *
   * @return A completable future to be called once complete.
   */
  public CompletableFuture<Void> start() {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    LOGGER.info("{} Starting context", clusterManager.localNode());
    transition(NoneController.class);
    checkConfiguration();
    return clusterManager.localNode().server().listen().whenComplete((result, error) -> {
      try {
        log.open();
      } catch (Exception e) {
        throw new CopycatException(e);
      }
      transition(FollowerController.class);
      events.start().handle(new StartEvent());
    });
  }

  /**
   * Checks the replica configuration and logs helpful warnings.
   */
  private void checkConfiguration() {
    if (clusterManager.remoteNodes().isEmpty()) {
      LOGGER.warn("{} - No remote nodes in the cluster!", clusterManager.localNode());
    }
    if (!config.isRequireQueryQuorum()) {
      LOGGER.warn("{} - Read quorums are disabled! This can cause stale reads!",
          clusterManager.localNode());
    }
    if (!config.isRequireCommandQuorum()) {
      LOGGER.warn("{} - Write quorums are disabled! This can cause data loss!",
          clusterManager.localNode());
    }
    if (config.getElectionTimeout() < config.getHeartbeatInterval()) {
      LOGGER.error("{} - Election timeout is greater than heartbeat interval!",
          clusterManager.localNode());
    }
  }

  /**
   * Stops the state.
   *
   * @return A completable future to be called once complete.
   */
  public CompletableFuture<Void> stop() {
    LOGGER.info("{} - Stopping context", clusterManager.localNode());
    return clusterManager.localNode().server().close().whenComplete((result, error) -> {
      try {
        log.close();
      } catch (Exception e) {
        throw new CopycatException(e);
      }
      transition(NoneController.class);
      events.stop().handle(new StopEvent());
    });
  }

  /**
   * Transitions to a new state.
   * 
   * @throws NullPointerException if {@code type} is null
   */
  public synchronized void transition(Class<? extends StateController> type) {
    Assert.isNotNull(type, "type");
    if ((currentState == null && type == null)
        || (currentState != null && type != null && type.isAssignableFrom(currentState.getClass()))) {
      return;
    }

    final StateController oldState = currentState;
    try {
      currentState = type.newInstance();
      LOGGER.info("{} - Transitioning to {}", clusterManager.localNode(), currentState.state());
    } catch (InstantiationException | IllegalAccessException e) {
      // Log the exception.
    }
    if (oldState != null) {
      oldState.destroy();
      currentState.init(this);
    } else {
      currentState.init(this);
    }

    events.stateChange().handle(new StateChangeEvent(currentState.state()));
  }

  /**
   * Returns the current leader.
   */
  public String currentLeader() {
    return currentLeader;
  }

  /**
   * Sets the current leader.
   */
  public StateContext currentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      LOGGER.debug("{} - currentLeader: {}", clusterManager.localNode(), leader);
    }

    if (currentLeader == null && leader != null) {
      currentLeader = leader;
      events.leaderElect().handle(new LeaderElectEvent(currentTerm, clusterManager.node(currentLeader).member()));
    } else if (currentLeader != null && leader != null && !currentLeader.equals(leader)) {
      currentLeader = leader;
      events.leaderElect().handle(new LeaderElectEvent(currentTerm, clusterManager.node(currentLeader).member()));
    } else {
      currentLeader = leader;
    }
    return this;
  }

  /**
   * Returns a boolean indicating whether this node is leader.
   *
   * @return Indicates whether this node is the leader.
   */
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(clusterManager.localNode().member().id());
  }

  /**
   * Returns the current term.
   */
  public long currentTerm() {
    return currentTerm;
  }

  /**
   * Sets the current term.
   */
  public StateContext currentTerm(long term) {
    if (term > currentTerm) {
      currentTerm = term;
      LOGGER.debug("{} - currentTerm: {}", clusterManager.localNode(), term);
      lastVotedFor = null;
    }
    return this;
  }

  /**
   * Returns the candidate last voted for.
   */
  public String lastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the candidate last voted for.
   */
  public StateContext lastVotedFor(String candidate) {
    if (lastVotedFor == null || !lastVotedFor.equals(candidate)) {
      LOGGER.debug("{} - lastVotedFor: {}", clusterManager.localNode(), candidate);
    }
    lastVotedFor = candidate;
    return this;
  }

  /**
   * Returns the commit index.
   */
  public long commitIndex() {
    return commitIndex;
  }

  /**
   * Sets the commit index.
   */
  public StateContext commitIndex(long index) {
    LOGGER.debug("{} - commitIndex: {}", clusterManager.localNode(), index);
    commitIndex = Math.max(commitIndex, index);
    return this;
  }

  /**
   * Returns the last applied index.
   */
  public long lastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last applied index.
   */
  public StateContext lastApplied(long index) {
    LOGGER.debug("{} - lastApplied: {}", clusterManager.localNode(), index);
    lastApplied = index;
    return this;
  }

  /**
   * Returns the next correlation ID from the correlation ID generator.
   */
  public Object nextCorrelationId() {
    return config.getCorrelationStrategy().nextCorrelationId();
  }

  /**
   * Submits an operation to the state.
   *
   * @param operation The operation to submit.
   * @param args The operation arguments.
   * @param <R> The operation return type.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(final String operation, final Object... args) {
    final CompletableFuture<R> future = new CompletableFuture<>();
    if (currentState == null) {
      future.completeExceptionally(new CopycatException("Invalid copycat state"));
      return future;
    }

    currentState.submit(new SubmitRequest(nextCorrelationId(), operation, Arrays.asList(args))).whenComplete((response, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        if (response.status().equals(Response.Status.OK)) {
          future.complete((R) response.result());
        } else {
          future.completeExceptionally(response.error());
        }
      }
    });
    return future;
  }

  @Override
  public String toString() {
    String value = "StateContext";
    value += "[\n";
    value += String.format("memberId=%s", clusterManager.localNode().member().id());
    value += ",\n";
    value += String.format("state=%s", currentState.state());
    value += ",\n";
    value += String.format("term=%d", currentTerm);
    value += ",\n";
    value += String.format("leader=%s", currentLeader);
    value += ",\n";
    value += String.format("lastVotedFor=%s", lastVotedFor);
    value += ",\n";
    value += String.format("commitIndex=%s", commitIndex);
    value += ",\n";
    value += String.format("lastApplied=%s", lastApplied);
    value += "\n]";
    return value;
  }

}
