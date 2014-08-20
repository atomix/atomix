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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.election.ElectionContext;
import net.kuujo.copycat.election.impl.RaftElectionContext;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogFactory;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.state.StateContext;
import net.kuujo.copycat.state.StateListener;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftStateContext implements StateContext {
  private static final Logger logger = Logger.getLogger(StateContext.class.getCanonicalName());
  private final Executor executor = Executors.newCachedThreadPool();
  private final Set<StateListener> listeners = new HashSet<>();
  private final CopyCatContext context;
  private final ClusterConfig cluster = new ClusterConfig();
  private final LogFactory logFactory;
  private Log log;
  private final RaftElectionContext election = new RaftElectionContext();
  private volatile RaftState currentState;
  private volatile String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>();
  private boolean leaderConnected;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  public RaftStateContext(CopyCatContext context, LogFactory logFactory) {
    this.context = context;
    this.logFactory = logFactory;
  }

  @Override
  public void addListener(StateListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(StateListener listener) {
    listeners.remove(listener);
  }

  @Override
  public CopyCatContext context() {
    return context;
  }

  @Override
  public ClusterConfig cluster() {
    return cluster;
  }

  @Override
  public Log log() {
    return log;
  }

  @Override
  public ElectionContext election() {
    return election;
  }

  @Override
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(context.cluster().localMember());
  }

  /**
   * Starts the context.
   */
  public CompletableFuture<Void> start() {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    transition(None.class);
    log = logFactory.createLog(String.format("%s.log", context.cluster().localMember().name()));
    return context.cluster().localMember().protocol().server().start().whenCompleteAsync((result, error) -> {
      log.open();
      log.restore();
      transition(Follower.class);
    });
  }

  /**
   * Stops the context.
   */
  public CompletableFuture<Void> stop() {
    return context.cluster().localMember().protocol().server().stop().whenCompleteAsync((result, error) -> {
      log.close();
      log = null;
      transition(None.class);
    });
  }

  /**
   * Transitions to a new state.
   */
  public synchronized void transition(Class<? extends RaftState> type) {
    if (currentState == null) {
      cluster.setLocalMember(context.cluster().config().getLocalMember());
      cluster.setRemoteMembers(context.cluster().config().getRemoteMembers());
    } else if (type != null && type.isAssignableFrom(currentState.getClass())) {
      return;
    }

    logger.info(context.cluster().localMember() + " transitioning to " + type.toString());
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
    currentLeader = leader;
    election.setLeaderAndTerm(currentTerm, currentLeader);

    // When a new leader is elected, we create a client and connect to the leader.
    // Once this node is connected to the leader we can begin submitting commands.
    if (leader == null) {
      leaderConnected = false;
      leaderClient = null;
    } else if (!isLeader()) {
      leaderConnected = false;
      leaderClient = context.cluster().member(currentLeader).protocol().client();
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
    return context.config().getCorrelationStrategy().nextCorrelationId(context);
  }

  /**
   * Submits a command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param args An ordered list of command arguments.
   * @return A completable future to be completed once the result is received.
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
      executor.execute(() -> {
        ProtocolHandler handler = context.election().currentLeader().equals(context.cluster().localMember()) ? currentState : leaderClient;
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
