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

import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.MemberConfig;
import net.kuujo.copycat.cluster.RemoteMember;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.event.impl.DefaultEventHandlersRegistry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.state.State;
import net.kuujo.copycat.state.StateContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatStateContext implements StateContext {
  private static final Logger logger = Logger.getLogger(StateContext.class.getCanonicalName());
  private final Executor executor = Executors.newCachedThreadPool();
  private final StateMachine stateMachine;
  @SuppressWarnings("rawtypes")
  private final Cluster cluster;
  private final Log log;
  private final CopycatConfig config;
  private final DefaultEventHandlersRegistry events = new DefaultEventHandlersRegistry();
  private volatile CopycatState currentState;
  private volatile String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>(50);
  private boolean leaderConnected;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <P extends Protocol<M>, M extends MemberConfig> CopycatStateContext(StateMachine stateMachine, Log log, ClusterConfig<M> cluster, P protocol, CopycatConfig config) {
    this.stateMachine = stateMachine;
    this.log = log;
    this.config = config;
    this.cluster = new Cluster(protocol, cluster);
  }

  @Override
  public StateMachine stateMachine() {
    return stateMachine;
  }

  @Override
  public CopycatConfig config() {
    return config;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Cluster cluster() {
    return cluster;
  }

  @Override
  public Log log() {
    return log;
  }

  @Override
  public DefaultEventHandlersRegistry events() {
    return events;
  }

  @Override
  public State.Type state() {
    return currentState.type();
  }

  @Override
  public String leader() {
    return currentLeader;
  }

  @Override
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(cluster.localMember().id());
  }

  @Override
  public CompletableFuture<Void> start() {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    transition(None.class);
    return cluster.localMember().server().start().whenCompleteAsync((result, error) -> {
      try {
        log.open();
      } catch (Exception e) {
        throw new CopycatException(e);
      }
      transition(Follower.class);
      events.start().handle(new StartEvent());
    });
  }

  @Override
  public CompletableFuture<Void> stop() {
    return cluster.localMember().server().stop().whenCompleteAsync((result, error) -> {
      try {
        log.close();
      } catch (Exception e) {
        throw new CopycatException(e);
      }
      transition(None.class);
      events.stop().handle(new StopEvent());
    });
  }

  /**
   * Transitions to a new state.
   */
  public synchronized void transition(Class<? extends CopycatState> type) {
    if (currentState == null) {
      cluster.update(cluster.config(), null);
    } else if (type != null && type.isAssignableFrom(currentState.getClass())) {
      return;
    }

    logger.info(cluster.localMember() + " transitioning to " + type.toString());
    final CopycatState oldState = currentState;
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

    events.stateChange().handle(new StateChangeEvent(currentState.type()));
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
  public CopycatStateContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
    }

    if (currentLeader == null && leader != null) {
      currentLeader = leader;
      events.leaderElect().handle(new LeaderElectEvent(currentTerm, cluster.member(currentLeader)));
    } else if (currentLeader != null && leader != null && !currentLeader.equals(leader)) {
      currentLeader = leader;
      events.leaderElect().handle(new LeaderElectEvent(currentTerm, cluster.member(currentLeader)));
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
      leaderClient = cluster.<RemoteMember<?>>member(currentLeader).client();
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
  public CopycatStateContext setCurrentTerm(long term) {
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
  public CopycatStateContext setLastVotedFor(String candidate) {
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
  public CopycatStateContext setCommitIndex(long index) {
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
  public CopycatStateContext setLastApplied(long index) {
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
      future.completeExceptionally(new CopycatException("No leader available"));
    } else if (!leaderConnected) {
      leaderConnectCallbacks.add(() -> {
        ProtocolHandler handler = leaderClient != null ? leaderClient : currentState;
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
        ProtocolHandler handler = currentLeader.equals(cluster.localMember().id()) ? currentState : leaderClient;
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
    return String.format("RaftStateContext[term=%d, leader=%s]", currentTerm, currentLeader);
  }

}
