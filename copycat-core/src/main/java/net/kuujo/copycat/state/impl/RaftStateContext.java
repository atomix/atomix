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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import net.kuujo.copycat.CopyCatConfig;
import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.RaftCluster;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.event.impl.DefaultEvents;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogFactory;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.state.StateContext;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftStateContext implements StateContext {
  private static final Logger logger = Logger.getLogger(StateContext.class.getCanonicalName());
  private final Executor executor = Executors.newCachedThreadPool();
  private final StateMachine stateMachine;
  private final Cluster cluster;
  private final ClusterConfig clusterConfig = new ClusterConfig();
  private final LogFactory logFactory;
  private final CopyCatConfig config;
  private final Registry registry;
  private final DefaultEvents events = new DefaultEvents();
  private Log log;
  private volatile RaftState currentState;
  private volatile String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>();
  private boolean leaderConnected;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  public RaftStateContext(StateMachine stateMachine, LogFactory logFactory, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.stateMachine = stateMachine;
    this.logFactory = logFactory;
    this.config = config;
    this.registry = registry;
    this.cluster = new RaftCluster(cluster, clusterConfig, this);
  }

  @Override
  public StateMachine stateMachine() {
    return stateMachine;
  }

  @Override
  public CopyCatConfig config() {
    return config;
  }

  /**
   * Returns the user-defined cluster configuration.
   *
   * @return The user-defined cluster configuration.
   */
  public ClusterConfig clusterConfig() {
    return clusterConfig;
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  public Log log() {
    return log;
  }

  @Override
  public Registry registry() {
    return registry;
  }

  @Override
  public DefaultEvents events() {
    return events;
  }

  @Override
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(cluster.localMember().uri());
  }

  @Override
  public CompletableFuture<Void> start() {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    transition(None.class);
    log = logFactory.createLog(String.format("%s.log", cluster.localMember().name()));
    return cluster.localMember().protocol().server().start().whenCompleteAsync((result, error) -> {
      log.open();
      log.restore();
      transition(Follower.class);
      events.start().run(new StartEvent());
    });
  }

  @Override
  public CompletableFuture<Void> stop() {
    return cluster.localMember().protocol().server().stop().whenCompleteAsync((result, error) -> {
      log.close();
      log = null;
      transition(None.class);
      events.stop().run(new StopEvent());
    });
  }

  /**
   * Transitions to a new state.
   */
  public synchronized void transition(Class<? extends RaftState> type) {
    if (currentState == null) {
      clusterConfig.setLocalMember(cluster.config().getLocalMember());
      clusterConfig.setRemoteMembers(cluster.config().getRemoteMembers());
    } else if (type != null && type.isAssignableFrom(currentState.getClass())) {
      return;
    }

    logger.info(cluster.localMember() + " transitioning to " + type.toString());
    final RaftState oldState = currentState;
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

    events.stateChange().run(new StateChangeEvent(currentState.type()));
  }

  /**
   * Returns the current leader.
   */
  public String getCurrentLeader() {
    return currentLeader;
  }

  /**
   * Sets the current leader.
   */
  public RaftStateContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
    }

    if (currentLeader == null && leader != null) {
      currentLeader = leader;
      events.leaderElect().run(new LeaderElectEvent(currentTerm, currentLeader));
    } else if (currentLeader != null && leader != null && !currentLeader.equals(leader)) {
      currentLeader = leader;
      events.leaderElect().run(new LeaderElectEvent(currentTerm, currentLeader));
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
      leaderClient = cluster.member(currentLeader).protocol().client();
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
  public long getCurrentTerm() {
    return currentTerm;
  }

  /**
   * Sets the current term.
   */
  public RaftStateContext setCurrentTerm(long term) {
    if (term > currentTerm) {
      currentTerm = term;
      logger.finer(String.format("Updated current term %d", term));
      lastVotedFor = null;
    }
    return this;
  }

  /**
   * Returns the candidate last voted for.
   */
  public String getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the candidate last voted for.
   */
  public RaftStateContext setLastVotedFor(String candidate) {
    if (lastVotedFor == null || !lastVotedFor.equals(candidate)) {
      logger.finer(String.format("Voted for %s", candidate));
    }
    lastVotedFor = candidate;
    return this;
  }

  /**
   * Returns the commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the commit index.
   */
  public RaftStateContext setCommitIndex(long index) {
    commitIndex = Math.max(commitIndex, index);
    return this;
  }

  /**
   * Returns the last applied index.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last applied index.
   */
  public RaftStateContext setLastApplied(long index) {
    lastApplied = index;
    return this;
  }

  /**
   * Returns the next correlation ID from the correlation ID generator.
   */
  public Object nextCorrelationId() {
    return config.getCorrelationStrategy().nextCorrelationId();
  }

  @Override
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
      executor.execute(() -> {
        ProtocolHandler handler = currentLeader.equals(cluster.localMember().uri()) ? currentState : leaderClient;
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
      });
    }
    return future;
  }

}
