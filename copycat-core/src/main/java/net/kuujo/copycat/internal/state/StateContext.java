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
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.MemberConfig;
import net.kuujo.copycat.cluster.RemoteMember;
import net.kuujo.copycat.event.LeaderElectEvent;
import net.kuujo.copycat.event.StartEvent;
import net.kuujo.copycat.event.StateChangeEvent;
import net.kuujo.copycat.event.StopEvent;
import net.kuujo.copycat.event.internal.DefaultEventHandlersRegistry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.RequestHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.spi.protocol.ProtocolClient;
import net.kuujo.copycat.util.Args;

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
public final class StateContext {
  private static final Logger logger = Logger.getLogger(StateContext.class.getCanonicalName());
  private final Executor executor = Executors.newCachedThreadPool();
  private final StateMachine stateMachine;
  @SuppressWarnings("rawtypes")
  private final Cluster cluster;
  private final Log log;
  private final CopycatConfig config;
  private final DefaultEventHandlersRegistry events = new DefaultEventHandlersRegistry();
  private volatile StateController currentState;
  private volatile String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>(50);
  private boolean leaderConnected;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <P extends Protocol<M>, M extends MemberConfig> StateContext(StateMachine stateMachine, Log log, ClusterConfig<M> cluster, P protocol, CopycatConfig config) {
    this.stateMachine = stateMachine;
    this.log = log;
    this.config = config;
    this.cluster = new Cluster(protocol, cluster);
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
  public DefaultEventHandlersRegistry events() {
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
    transition(NoneController.class);
    return cluster.localMember().server().start().whenCompleteAsync((result, error) -> {
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
   * Stops the state.
   *
   * @return A completable future to be called once complete.
   */
  public CompletableFuture<Void> stop() {
    return cluster.localMember().server().stop().whenCompleteAsync((result, error) -> {
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
    if (currentState == null) {
      cluster.update(cluster.config(), null);
    } else if (type != null && type.isAssignableFrom(currentState.getClass())) {
      return;
    }

    logger.info(cluster.localMember() + " transitioning to " + type.toString());
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
   * Returns a boolean indicating whether this node is leader.
   *
   * @return Indicates whether this node is the leader.
   */
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(cluster.localMember().id());
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
      logger.finer(String.format("Updated current term %d", term));
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
      logger.finer(String.format("Voted for %s", candidate));
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
        RequestHandler handler = currentLeader.equals(cluster.localMember().id()) ? currentState : leaderClient;
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
    value += String.format("memberId=%s", cluster.localMember().id());
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
