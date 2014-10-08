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

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.internal.cluster.ClusterManager;
import net.kuujo.copycat.internal.cluster.RemoteNode;
import net.kuujo.copycat.internal.event.DefaultEventHandlers;
import net.kuujo.copycat.internal.util.Args;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.RequestHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.spi.protocol.ProtocolClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class StateContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(StateContext.class);
  private final Executor executor = Executors.newCachedThreadPool();
  private final StateMachine stateMachine;
  @SuppressWarnings("rawtypes")
  private final Cluster cluster;
  @SuppressWarnings("rawtypes")
  private final ClusterManager clusterManager;
  private final Log log;
  private final CopycatConfig config;
  private final DefaultEventHandlers events = new DefaultEventHandlers();
  private volatile StateController currentState;
  private volatile String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>(50);
  private boolean leaderConnected;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  public <M extends Member> StateContext(StateMachine stateMachine, Log log, Cluster<M> cluster, CopycatConfig config) {
    this.stateMachine = stateMachine;
    this.log = log;
    this.config = config;
    this.cluster = cluster;
    this.clusterManager = new ClusterManager<>(cluster);
  }

  /**
   * Returns the state machine.
   *
   * @return The state machine.
   */
  public StateMachine stateMachine() {
    return stateMachine;
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
  @SuppressWarnings("rawtypes")
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the copycat cluster manager.
   *
   * @return The copycat cluster manager.
   */
  @SuppressWarnings("rawtypes")
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
    LOGGER.info("{} starting context", clusterManager.localNode().member());
    transition(NoneController.class);
    checkConfiguration();
    return clusterManager.localNode().server().listen().whenCompleteAsync((result, error) -> {
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
      LOGGER.warn("{} No remote nodes in the cluster!", clusterManager.localNode().member());
    }
    if (!config.isRequireReadQuorum()) {
      LOGGER.warn("{} Read quorums are disabled! This can cause stale reads!", clusterManager.localNode().member());
    }
    if (!config.isRequireWriteQuorum()) {
      LOGGER.warn("{} Write quorums are disabled! This can cause data loss!", clusterManager.localNode().member());
    }
    if (config.getElectionTimeout() < config.getHeartbeatInterval()) {
      LOGGER.error("{} Election timeout is greater than heartbeat interval!", clusterManager.localNode().member());
    }
  }

  /**
   * Stops the state.
   *
   * @return A completable future to be called once complete.
   */
  public CompletableFuture<Void> stop() {
    LOGGER.info("{} stopping context", clusterManager.localNode().member());
    return clusterManager.localNode().server().close().whenCompleteAsync((result, error) -> {
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
   */
  public synchronized void transition(Class<? extends StateController> type) {
    Args.checkNotNull(type);
    if ((currentState == null && type == null) || (currentState != null && type != null && type.isAssignableFrom(currentState.getClass()))) {
      return;
    }

    LOGGER.info("{} transitioning: {}", clusterManager.localNode().member(), type);
    final StateController oldState = currentState;
    try {
      currentState = type.newInstance();
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
      LOGGER.debug("{} currentLeader: {}", clusterManager.localNode().member(), leader);
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

    // When a new leader is elected, we create a client and connect to the leader.
    // Once this node is connected to the leader we can begin submitting commands.
    if (leader == null) {
      leaderConnected = false;
      leaderClient = null;
    } else if (!isLeader()) {
      leaderConnected = false;
      leaderClient = ((RemoteNode<?>) clusterManager.node(currentLeader)).client();
      leaderClient.connect().thenRun(() -> {
        leaderConnected = true;
        runLeaderConnectCallbacks();
      });
    } else {
      leaderConnected = true;
      runLeaderConnectCallbacks();
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
   * Runs leader connect callbacks.
   */
  private void runLeaderConnectCallbacks() {
    Iterator<Runnable> iterator = leaderConnectCallbacks.iterator();
    while (iterator.hasNext()) {
      Runnable runnable = iterator.next();
      iterator.remove();
      runnable.run();
    }
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
      LOGGER.debug("{} currentTerm: {}", clusterManager.localNode().member(), term);
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
      LOGGER.debug("{} lastVotedFor: {}", clusterManager.localNode().member(), candidate);
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
    LOGGER.debug("{} lastApplied: {}", clusterManager.localNode().member(), index);
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
   * Submits a command to the state.
   *
   * @param command The command to submit.
   * @param args The command arguments.
   * @param <R> The command return type.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submitCommand(final String command, final Object... args) {
    CompletableFuture<R> future = new CompletableFuture<>();
    if (currentLeader == null) {
      future.completeExceptionally(new CopycatException("No leader available"));
    } else if (!leaderConnected) {
      leaderConnectCallbacks.add(() -> {
        RequestHandler handler = leaderClient != null ? leaderClient : currentState;
        handler.submit(new SubmitRequest(nextCorrelationId(), command, Arrays.asList(args))).whenComplete((result, error) -> {
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
      executor.execute(() -> {
        RequestHandler handler = currentLeader.equals(clusterManager.localNode().member().id()) ? currentState : leaderClient;
        handler.submit(new SubmitRequest(nextCorrelationId(), command, Arrays.asList(args))).whenComplete((result, error) -> {
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
    }
    return future;
  }

  @Override
  public String toString() {
    String value = "CopycatConfig";
    value += "[\n";
    value += String.format("memberId=%s", clusterManager.localNode().member().id());
    value += ",\n";
    value += String.format("state=%s", currentState);
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
