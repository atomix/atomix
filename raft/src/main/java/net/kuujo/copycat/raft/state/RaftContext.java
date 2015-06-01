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

import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.log.Compactor;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.raft.rpc.CommandRequest;
import net.kuujo.copycat.raft.rpc.QueryRequest;
import net.kuujo.copycat.raft.rpc.Response;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.ThreadChecker;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftContext implements Managed<RaftContext> {
  private final Logger LOGGER = LoggerFactory.getLogger(RaftContext.class);
  private final RaftStateMachine stateMachine;
  private final Log log;
  private final Compactor compactor;
  private final ManagedCluster cluster;
  private final String topic;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private AbstractState state;
  private CompletableFuture<RaftContext> openFuture;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private long keepAliveInterval = 2000;
  private long session;
  private long request;
  private long response;
  private int leader;
  private long term;
  private int lastVotedFor;
  private volatile long commitIndex;
  private volatile long globalIndex;
  private volatile long lastApplied;
  private volatile boolean open;

  public RaftContext(Log log, StateMachine stateMachine, ManagedCluster cluster, String topic, ExecutionContext context) {
    this.log = log;
    this.stateMachine = new RaftStateMachine(stateMachine);
    this.compactor = new Compactor(log, this.stateMachine::filter, context);
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
   * Sets the client keep alive interva.
   *
   * @param keepAliveInterval The client keep alive interval in milliseconds.
   * @return The Raft context.
   */
  public RaftContext setKeepAliveInterval(long keepAliveInterval) {
    this.keepAliveInterval = keepAliveInterval;
    return this;
  }

  /**
   * Returns the client keep alive interval.
   *
   * @return The keep alive interval in milliseconds.
   */
  public long getKeepAliveInterval() {
    return keepAliveInterval;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout in milliseconds.
   * @return The Raft context.
   */
  public RaftContext setSessionTimeout(long sessionTimeout) {
    stateMachine.setSessionTimeout(sessionTimeout);
    return this;
  }

  /**
   * Sets the state session.
   *
   * @param session The state session.
   * @return The Raft context.
   */
  RaftContext setSession(long session) {
    this.session = session;
    this.request = 0;
    this.response = 0;
    if (session != 0 && openFuture != null) {
      synchronized (openFuture) {
        if (openFuture != null) {
          CompletableFuture<RaftContext> future = openFuture;
          context.execute(() -> future.complete(this));
          openFuture = null;
        }
      }
    }
    return this;
  }

  /**
   * Returns the state session.
   *
   * @return The state session.
   */
  public long getSession() {
    return session;
  }

  /**
   * Sets the session request number.
   *
   * @param request The session request number.
   * @return The Raft context.
   */
  RaftContext setRequest(long request) {
    this.request = request;
    return this;
  }

  /**
   * Returns the next request number.
   *
   * @return The next request number.
   */
  long nextRequest() {
    return ++request;
  }

  /**
   * Returns the session request number.
   *
   * @return The session request number.
   */
  public long getRequest() {
    return request;
  }

  /**
   * Sets the session response number.
   *
   * @param response The session response number.
   * @return The Raft context.
   */
  RaftContext setResponse(long response) {
    this.response = response;
    return this;
  }

  /**
   * Returns the session response number.
   *
   * @return The session response number.
   */
  public long getResponse() {
    return response;
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
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
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
      throw new IllegalArgumentException("global index must be positive");
    if (globalIndex < this.globalIndex)
      throw new IllegalArgumentException("cannot decrease global index");
    this.globalIndex = globalIndex;
    compactor.setCompactIndex(globalIndex);
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
    compactor.setCommitIndex(lastApplied);
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
  RaftStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  public Log getLog() {
    return log;
  }

  /**
   * Returns the log compactor.
   *
   * @return The log compactor.
   */
  public Compactor getCompactor() {
    return compactor;
  }

  /**
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    threadChecker.checkThread();
  }

  /**
   * Submits a command.
   *
   * @param command The command to submit.
   * @param <R> The command result type.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(Command<R> command) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<R> future = new CompletableFuture<>();
    context.execute(() -> {
      // TODO: This should retry on timeouts with the same request ID.
      long requestId = nextRequest();
      CommandRequest request = CommandRequest.builder()
        .withSession(getSession())
        .withRequest(requestId)
        .withResponse(getResponse())
        .withCommand(command)
        .build();
      state.command(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete((R) response.result());
          } else {
            future.completeExceptionally(response.error().createException());
          }
          setResponse(Math.max(getResponse(), requestId));
        } else {
          future.completeExceptionally(error);
        }
        request.close();
      });
    });
    return future;
  }

  /**
   * Submits a query.
   *
   * @param query The query to submit.
   * @param <R> The query result type.
   * @return A completable future to be completed with the query result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(Query<R> query) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<R> future = new CompletableFuture<>();
    context.execute(() -> {
      QueryRequest request = QueryRequest.builder()
        .withSession(getSession())
        .withQuery(query)
        .build();
      state.query(request).whenComplete((response, error) -> {
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
    if (openFuture != null)
      return openFuture;

    openFuture = new CompletableFuture<>();
    cluster.open().whenCompleteAsync((result, error) -> {
      if (error == null) {
        open = true;
        switch (cluster.member().type()) {
          case CLIENT:
            transition(RemoteState.class);
            break;
          case ACTIVE:
            log.open(context);
            transition(FollowerState.class);
            break;
        }
      } else {
        openFuture.completeExceptionally(error);
        openFuture = null;
      }
    }, context);
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
      transition(StartState.class).thenCompose(v -> cluster.close()).whenComplete((result, error) -> {
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
