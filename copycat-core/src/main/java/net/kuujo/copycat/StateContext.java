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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;

/**
 * State context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateContext implements EventProvider<StateListener> {
  private static final Logger logger = Logger.getLogger(StateContext.class.getCanonicalName());
  private final Set<StateListener> listeners = new HashSet<>();
  final CopyCatContext context;
  final ClusterConfig cluster = new ClusterConfig();
  private volatile State currentState;
  private volatile String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Runnable> leaderConnectCallbacks = new ArrayList<>();
  private boolean leaderConnected;
  private volatile long currentTerm;
  private volatile String lastVotedFor;
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  StateContext(CopyCatContext context) {
    this.context = context;
  }

  @Override
  public void addListener(StateListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(StateListener listener) {
    listeners.remove(listener);
  }

  /**
   * Returns a boolean indicating whether this node is the leader.
   *
   * @return Indicates whether this node is the leader.
   */
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(context.cluster.localMember());
  }

  /**
   * Transitions the context to a new state.
   *
   * @param type The state to which to transition.
   */
  synchronized void transition(Class<? extends State> type) {
    if (currentState == null) {
      cluster.setLocalMember(context.cluster.config().getLocalMember());
      cluster.setRemoteMembers(context.cluster.config().getRemoteMembers());
    } else if (type != null && type.isAssignableFrom(currentState.getClass())) {
      return;
    }

    logger.info(context.cluster.localMember() + " transitioning to " + type.toString());
    final State oldState = currentState;
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


  String getCurrentLeader() {
    return currentLeader;
  }

  StateContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
    }
    currentLeader = leader;
    context.election.setLeaderAndTerm(currentTerm, currentLeader);

    // When a new leader is elected, we create a client and connect to the leader.
    // Once this node is connected to the leader we can begin submitting commands.
    if (leader == null) {
      leaderConnected = false;
      leaderClient = null;
    } else if (!isLeader()) {
      leaderConnected = false;
      leaderClient = context.cluster.member(currentLeader).protocol().client();
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

  long getCurrentTerm() {
    return currentTerm;
  }

  StateContext setCurrentTerm(long term) {
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

  StateContext setLastVotedFor(String candidate) {
    if (lastVotedFor == null || !lastVotedFor.equals(candidate)) {
      logger.finer(String.format("Voted for %s", candidate));
    }
    lastVotedFor = candidate;
    return this;
  }

  long getCommitIndex() {
    return commitIndex;
  }

  StateContext setCommitIndex(long index) {
    commitIndex = Math.max(commitIndex, index);
    return this;
  }

  long getLastApplied() {
    return lastApplied;
  }

  StateContext setLastApplied(long index) {
    lastApplied = index;
    return this;
  }

  Object nextCorrelationId() {
    return context.config.getCorrelationStrategy().nextCorrelationId(context);
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
      ProtocolHandler handler = context.election.currentLeader().equals(context.cluster.localMember()) ? currentState : leaderClient;
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
