/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.log.RaftLog;
import net.kuujo.copycat.raft.rpc.Response;
import net.kuujo.copycat.raft.rpc.SubmitRequest;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.ThreadChecker;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftContext implements Managed<RaftContext> {
  private final Logger LOGGER = LoggerFactory.getLogger(RaftContext.class);
  private final Set<EventListener<Long>> termListeners = new CopyOnWriteArraySet<>();
  private final Set<EventListener<Member>> electionListeners = new CopyOnWriteArraySet<>();
  private final StateMachineProxy stateMachine;
  private final RaftLog log;
  private final ManagedCluster cluster;
  private final String topic;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private AbstractState state;
  private CompletableFuture<RaftContext> openFuture;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private int leader;
  private long term;
  private int lastVotedFor;
  private volatile long firstCommitIndex = 0;
  private volatile long commitIndex = 0;
  private volatile long globalIndex = 0;
  private volatile long lastApplied = 0;
  private volatile boolean open;

  public RaftContext(RaftLog log, StateMachine stateMachine, ManagedCluster cluster, String topic, ExecutionContext context) {
    this.log = log;
    this.stateMachine = new StateMachineProxy(stateMachine, this, new ExecutionContext(context.name() + "-state"));
    this.cluster = cluster;
    this.topic = topic;
    this.context = context;
    this.threadChecker = new ThreadChecker(context);
  }

  /**
   * Returns the Raft cluster.
   *
   * @return The Raft cluster.
   */
  public ManagedCluster getCluster() {
    return cluster;
  }

  /**
   * Returns the topic on which the Raft instance communicates.
   *
   * @return The topic on which the Raft instance communicates.
   */
  public String getTopic() {
    return topic;
  }

  /**
   * Returns the command serializer.
   *
   * @return The command serializer.
   */
  public Serializer getSerializer() {
    return cluster.serializer();
  }

  /**
   * Returns the execution context.
   *
   * @return The execution context.
   */
  public ExecutionContext getContext() {
    return context;
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout The election timeout.
   * @return The Raft context.
   */
  public RaftContext setElectionTimeout(long electionTimeout) {
    this.electionTimeout = electionTimeout;
    return this;
  }

  /**
   * Returns the election timeout.
   *
   * @return The election timeout.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @return The Raft context.
   */
  public RaftContext setHeartbeatInterval(long heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
    return this;
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return The heartbeat interval in milliseconds.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Adds a term change listener.
   *
   * @param listener The term change listener.
   * @return The Raft protocol.
   */
  public RaftContext addTermListener(EventListener<Long> listener) {
    termListeners.add(listener);
    return this;
  }

  /**
   * Removes a term change listener.
   *
   * @param listener The term change listener.
   * @return The Raft protocol.
   */
  public RaftContext removeTermListener(EventListener<Long> listener) {
    termListeners.remove(listener);
    return this;
  }

  /**
   * Adds an election listener.
   *
   * @param listener The election listener.
   * @return The Raft protocol.
   */
  public RaftContext addElectionListener(EventListener<Member> listener) {
    electionListeners.add(listener);
    return this;
  }

  /**
   * Removes an election listener.
   *
   * @param listener The election listener.
   * @return The Raft protocol.
   */
  public RaftContext removeElectionListener(EventListener<Member> listener) {
    electionListeners.remove(listener);
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  RaftContext setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
        electionListeners.forEach(l -> l.accept(cluster.member(this.leader)));
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
        electionListeners.forEach(l -> l.accept(cluster.member(this.leader)));
      }
    } else {
      this.leader = 0;
    }
    return this;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  public int getLeader() {
    return leader;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  RaftContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = 0;
      this.lastVotedFor = 0;
      LOGGER.debug("{} - Incremented term {}", cluster.member().id(), term);
      termListeners.forEach(l -> l.accept(this.term));
    }
    return this;
  }

  /**
   * Returns the state term.
   *
   * @return The state term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  RaftContext setLastVotedFor(int candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    if (lastVotedFor != 0 && candidate != 0) {
      throw new IllegalStateException("Already voted for another candidate");
    }
    if (leader != 0 && candidate != 0) {
      throw new IllegalStateException("Cannot cast vote - leader already exists");
    }
    this.lastVotedFor = candidate;
    if (candidate != 0) {
      LOGGER.debug("{} - Voted for {}", cluster.member().id(), candidate);
    } else {
      LOGGER.debug("{} - Reset last voted for", cluster.member().id());
    }
    return this;
  }

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  public int getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return The Raft context.
   */
  RaftContext setCommitIndex(long commitIndex) {
    if (commitIndex < 0)
      throw new IllegalArgumentException("commit index must be positive");
    if (commitIndex < this.commitIndex)
      throw new IllegalArgumentException("cannot decrease commit index");
    if (firstCommitIndex == 0) {
      if (commitIndex == 0) {
        if (openFuture != null) {
          synchronized (openFuture) {
            if (openFuture != null) {
              openFuture.complete(this);
              openFuture = null;
            }
          }
        }
      } else {
        firstCommitIndex = commitIndex;
      }
    }
    this.commitIndex = commitIndex;
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the recycle index.
   *
   * @param globalIndex The recycle index.
   * @return The Raft context.
   */
  RaftContext setGlobalIndex(long globalIndex) {
    if (globalIndex < 0)
      throw new IllegalArgumentException("recycle index must be positive");
    if (globalIndex < this.globalIndex)
      throw new IllegalArgumentException("cannot decrease recycle index");
    this.globalIndex = globalIndex;
    return this;
  }

  /**
   * Returns the recycle index.
   *
   * @return The state recycle index.
   */
  public long getGlobalIndex() {
    return globalIndex;
  }

  /**
   * Sets the state last applied index.
   *
   * @param lastApplied The state last applied index.
   * @return The Raft context.
   */
  RaftContext setLastApplied(long lastApplied) {
    if (lastApplied < 0)
      throw new IllegalArgumentException("last applied must be positive");
    if (lastApplied < this.lastApplied)
      throw new IllegalArgumentException("cannot decrease last applied");
    if (lastApplied > commitIndex)
      throw new IllegalArgumentException("last applied cannot be greater than commit index");
    this.lastApplied = lastApplied;
    if (openFuture != null) {
      synchronized (openFuture) {
        if (openFuture != null && this.lastApplied != 0 && firstCommitIndex != 0 && this.lastApplied >= firstCommitIndex) {
          CompletableFuture<RaftContext> future = openFuture;
          context.execute(() -> future.complete(this));
          openFuture = null;
        }
      }
    }
    return this;
  }

  /**
   * Returns the state last applied index.
   *
   * @return The state last applied index.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  public RaftState getState() {
    return state.type();
  }

  /**
   * Returns the state machine proxy.
   *
   * @return The state machine proxy.
   */
  StateMachineProxy getStateMachine() {
    return stateMachine;
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  public RaftLog getLog() {
    return log;
  }

  /**
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    threadChecker.checkThread();
  }

  /**
   * Submits an operation.
   *
   * @param operation The operation to submit.
   * @param <R> The operation result type.
   * @return A completable future to be completed with the operation result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(Operation<R> operation) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<R> future = new CompletableFuture<>();
    SubmitRequest request = SubmitRequest.builder()
      .withOperation(operation)
      .build();
    context.execute(() -> {
      state.submit(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete((R) response.result());
          } else {
            future.completeExceptionally(response.error().createException());
          }
        } else {
          future.completeExceptionally(error);
        }
        request.close();
      });
    });
    return future;
  }

  /**
   * Transition handler.
   */
  CompletableFuture<RaftState> transition(Class<? extends AbstractState> state) {
    checkThread();

    if (this.state != null && state == this.state.getClass()) {
      return CompletableFuture.completedFuture(this.state.type());
    }

    LOGGER.info("{} - Transitioning to {}", cluster.member().id(), state);

    // Force state transitions to occur synchronously in order to prevent race conditions.
    if (this.state != null) {
      try {
        this.state.close().get();
        this.state = state.getConstructor(RaftContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.getConstructor(RaftContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<RaftContext> open() {
    if (openFuture != null) {
      return openFuture;
    }

    openFuture = new CompletableFuture<>();

    context.execute(() -> {
      try {
        open = true;
        switch (cluster.member().type()) {
          case CLIENT:
            transition(RemoteState.class);
            break;
          case PASSIVE:
            log.open();
            transition(PassiveState.class);
            break;
          case ACTIVE:
            log.open();
            transition(FollowerState.class);
            break;
        }
      } catch (Exception e) {
        openFuture.completeExceptionally(e);
        openFuture = null;
      }
    });
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (openFuture != null) {
      openFuture.cancel(false);
      openFuture = null;
    } else if (!open) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Context not open"));
      return future;
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      transition(StartState.class).whenComplete((result, error) -> {
        if (error == null) {
          try {
            if (log != null)
              log.close();
            future.complete(null);
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        } else {
          try {
            if (log != null)
              log.close();
            future.completeExceptionally(error);
          } catch (Exception e) {
            future.completeExceptionally(error);
          }
        }
      });
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Deletes the context.
   */
  public CompletableFuture<Void> delete() {
    if (open)
      return Futures.exceptionalFuture(new IllegalStateException("cannot delete open context"));

    return CompletableFuture.runAsync(() -> {
      if (log != null)
        log.delete();
    }, context);
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
