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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderResolver;
import net.kuujo.copycat.io.storage.Entry;
import net.kuujo.copycat.io.storage.Log;
import net.kuujo.copycat.io.transport.Connection;
import net.kuujo.copycat.io.transport.Server;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.RaftServer;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.raft.server.storage.*;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerContext implements Managed<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerContext.class);
  private final Listeners<RaftServer.State> listeners = new Listeners<>();
  private final Serializer serializer;
  private Context context;
  private final StateMachine stateMachine;
  private ServerStateMachineExecutor stateExecutor;
  private final Member member;
  private final Log log;
  private final ClusterState cluster;
  private final Members members;
  private final Transport transport;
  private Server server;
  private ConnectionManager connections;
  private ServerCommitPool commits;
  private AbstractState state;
  private long electionTimeout = 500;
  private long sessionTimeout = 5000;
  private long heartbeatInterval = 250;
  private int leader;
  private long term;
  private long lastApplied;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;
  private volatile boolean open;
  private volatile CompletableFuture<Void> openFuture;

  public ServerContext(int memberId, Members members, Transport transport, Log log, StateMachine stateMachine, Serializer serializer) {
    Member member = members.member(memberId);
    if (member == null) {
      throw new ConfigurationException("active member must be listed in members list");
    }

    if (member.host() == null) {
      throw new ConfigurationException("member host not configured");
    }
    if (member.port() <= 0) {
      throw new ConfigurationException("member port not configured");
    }

    this.transport = transport;
    this.cluster = new ClusterState(this, member);
    this.members = members;
    this.member = member;
    this.serializer = serializer;

    log.serializer().resolve(new ServiceLoaderResolver());
    serializer.resolve(new ServiceLoaderResolver());

    this.log = log;
    this.stateMachine = stateMachine;
  }

  /**
   * Registers a state change listener.
   *
   * @param listener The state change listener.
   * @return The listener context.
   */
  public ListenerContext<RaftServer.State> onStateChange(Listener<RaftServer.State> listener) {
    return listeners.add(listener);
  }

  /**
   * Returns the server member.
   *
   * @return The local server member.
   */
  public Member getMember() {
    return member;
  }

  /**
   * Returns the command serializer.
   *
   * @return The command serializer.
   */
  public Serializer getSerializer() {
    return context.serializer();
  }

  /**
   * Returns the execution context.
   *
   * @return The execution context.
   */
  public Context getContext() {
    return context;
  }

  /**
   * Returns the context connection manager.
   *
   * @return The context connection manager.
   */
  ConnectionManager getConnections() {
    return connections;
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout The election timeout.
   * @return The Raft context.
   */
  public ServerContext setElectionTimeout(long electionTimeout) {
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
  public ServerContext setHeartbeatInterval(long heartbeatInterval) {
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
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout.
   * @return The Raft state machine.
   */
  public ServerContext setSessionTimeout(long sessionTimeout) {
    if (sessionTimeout <= 0)
      throw new IllegalArgumentException("session timeout must be positive");

    this.sessionTimeout = sessionTimeout;
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  ServerContext setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", member.id(), leader);
        if (openFuture != null) {
          openFuture.complete(null);
          openFuture = null;
        }
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", member.id(), leader);
      }
    } else {
      this.leader = 0;
    }
    return this;
  }

  /**
   * Returns the cluster state.
   *
   * @return The cluster state.
   */
  ClusterState getCluster() {
    return cluster;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  public Member getLeader() {
    if (leader == 0) {
      return null;
    } else if (leader == member.id()) {
      return member;
    }

    MemberState member = cluster.getMember(leader);
    return member != null ? member.getMember() : null;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  ServerContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = 0;
      this.lastVotedFor = 0;
      LOGGER.debug("{} - Incremented term {}", member.id(), term);
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
  ServerContext setLastVotedFor(int candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    if (lastVotedFor != 0 && candidate != 0) {
      throw new IllegalStateException("Already voted for another candidate");
    }
    if (leader != 0 && candidate != 0) {
      throw new IllegalStateException("Cannot cast vote - leader already exists");
    }
    this.lastVotedFor = candidate;
    if (candidate != 0) {
      LOGGER.debug("{} - Voted for {}", member.id(), candidate);
    } else {
      LOGGER.debug("{} - Reset last voted for", member.id());
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
  ServerContext setCommitIndex(long commitIndex) {
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
  ServerContext setGlobalIndex(long globalIndex) {
    if (globalIndex < 0)
      throw new IllegalArgumentException("global index must be positive");
    this.globalIndex = Math.max(this.globalIndex, globalIndex);
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
   * Returns the last index applied to the state machine.
   *
   * @return The last index applied to the state machine.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last index applied to the state machine.
   *
   * @param lastApplied The last index applied to the state machine.
   */
  private void setLastApplied(long lastApplied) {
    this.lastApplied = lastApplied;
  }

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  public RaftServer.State getState() {
    return state.type();
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
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    context.checkThread();
  }

  /**
   * Transition handler.
   */
  CompletableFuture<RaftServer.State> transition(Class<? extends AbstractState> state) {
    checkThread();

    if (this.state != null && state == this.state.getClass()) {
      return CompletableFuture.completedFuture(this.state.type());
    }

    LOGGER.info("{} - Transitioning to {}", member.id(), state.getSimpleName());

    // Force state transitions to occur synchronously in order to prevent race conditions.
    if (this.state != null) {
      try {
        this.state.close().get();
        this.state = state.getConstructor(ServerContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.getConstructor(ServerContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }

    listeners.forEach(l -> l.accept(this.state.type()));
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Handles a connection.
   */
  private void handleConnect(Connection connection) {
    stateExecutor.context().sessions().registerConnection(connection);
    registerHandlers(connection);
    connection.closeListener(stateExecutor.context().sessions()::unregisterConnection);
  }

  /**
   * Registers all message handlers.
   */
  private void registerHandlers(Connection connection) {
    context.checkThread();

    // Note we do not use method references here because the "state" variable changes over time.
    // We have to use lambdas to ensure the request handler points to the current state.
    connection.handler(RegisterRequest.class, request -> state.register(request));
    connection.handler(KeepAliveRequest.class, request -> state.keepAlive(request));
    connection.handler(JoinRequest.class, request -> state.join(request));
    connection.handler(LeaveRequest.class, request -> state.leave(request));
    connection.handler(AppendRequest.class, request -> state.append(request));
    connection.handler(PollRequest.class, request -> state.poll(request));
    connection.handler(VoteRequest.class, request -> state.vote(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<?> apply(Entry entry) {
    if (entry instanceof CommandEntry) {
      return apply((CommandEntry) entry);
    } else if (entry instanceof QueryEntry) {
      return apply((QueryEntry) entry);
    } else if (entry instanceof RegisterEntry) {
      return apply((RegisterEntry) entry);
    } else if (entry instanceof KeepAliveEntry) {
      return apply((KeepAliveEntry) entry);
    } else if (entry instanceof ConfigurationEntry) {
      return apply((ConfigurationEntry) entry);
    } else if (entry instanceof NoOpEntry) {
      return apply((NoOpEntry) entry);
    }
    return Futures.exceptionalFuture(new InternalException("unknown state machine operation"));
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Long> apply(RegisterEntry entry) {
    ServerSession session = stateExecutor.context().sessions().registerSession(entry.getIndex(), entry.getConnection()).setTimestamp(entry.getTimestamp());

    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Long> future = new ComposableFuture<>();
    stateExecutor.execute(() -> {
      stateMachine.register(session);
      context.execute(() -> future.complete(entry.getIndex()));
    });

    setLastApplied(session.id());
    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   */
  CompletableFuture<Void> apply(KeepAliveEntry entry) {
    ServerSession session = stateExecutor.context().sessions().getSession(entry.getSession());

    CompletableFuture<Void> future;
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.getSession()));
    } else {
      if (entry.getTimestamp() - sessionTimeout > session.getTimestamp()) {
        LOGGER.warn("Expired session: " + entry.getSession());
        future = expireSession(entry.getSession());
      } else {
        session.setIndex(entry.getIndex()).setTimestamp(entry.getTimestamp()).clearCommands(entry.getSequence());
        future = new CompletableFuture<>();
        stateExecutor.tick(Instant.ofEpochMilli(entry.getTimestamp()));
        context.execute(() -> future.complete(null));
      }
    }

    setLastApplied(entry.getIndex());
    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Object> apply(CommandEntry entry) {
    final CompletableFuture<Object> future;

    // First check to ensure that the session exists.
    ServerSession session = stateExecutor.context().sessions().getSession(entry.getSession());
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.getSession()));
    } else if (entry.getTimestamp() - sessionTimeout > session.getTimestamp()) {
      LOGGER.warn("Expired session: " + entry.getSession());
      future = expireSession(entry.getSession());
    } else {
      session.setTimestamp(entry.getTimestamp());
      if (session.hasResponse(entry.getSequence())) {
        future = CompletableFuture.completedFuture(session.getResponse(entry.getSequence()));
      } else {
        // Create a server commit.
        ServerCommit commit = commits.acquire(entry);

        // Execute the state machine operation using the commit. Once complete, register the
        // operation output with the session. This will ensure linearizability for commands
        // applied to the state machine more than once.
        long sequence = entry.getSequence();
        future = execute(commit).thenApply(result -> {
          session.registerResponse(sequence, result);
          return result;
        });

        // Increment the session version.
        session.setVersion(entry.getSequence());
      }
    }

    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    setLastApplied(entry.getIndex());

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Object> apply(QueryEntry entry) {
    ServerSession session = stateExecutor.context().sessions().getSession(entry.getSession());
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.getSession()));
    } else if (entry.getTimestamp() - sessionTimeout > session.getTimestamp()) {
      LOGGER.warn("Expired session: " + entry.getSession());
      return expireSession(entry.getSession());
    } else if (session.getVersion() < entry.getSequence()) {
      ComposableFuture<Object> future = new ComposableFuture<>();
      session.registerQuery(entry.getSequence(), () -> execute(commits.acquire(entry), future));
      return future;
    } else {
      return execute(commits.acquire(entry));
    }
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Members> apply(ConfigurationEntry entry) {
    if (cluster.isPassive()) {
      cluster.configure(entry.getIndex(), entry.getActive(), entry.getPassive());
      if (cluster.isActive()) {
        transition(FollowerState.class);
      }
    } else {
      cluster.configure(entry.getIndex(), entry.getActive(), entry.getPassive());
      if (cluster.isPassive()) {
        transition(PassiveState.class);
      }
    }
    return Futures.completedFutureAsync(cluster.buildActiveMembers(), context);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Long> apply(NoOpEntry entry) {
    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    setLastApplied(entry.getIndex());
    return Futures.completedFutureAsync(entry.getIndex(), context);
  }

  /**
   * Expires a session.
   */
  private <T> CompletableFuture<T> expireSession(long sessionId) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ServerSession session = stateExecutor.context().sessions().unregisterSession(sessionId);
    if (session != null) {
      stateExecutor.execute(() -> {
        session.expire();
        stateMachine.expire(session);
        context.execute(() -> future.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId)));
      });
    } else {
      context.execute(() -> future.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId)));
    }
    return future;
  }

  /**
   * Executes a command in the state machine thread and completes the given future asynchronously in the server thread.
   */
  private <T extends Operation<U>, U> CompletableFuture<U> execute(Commit<T> commit) {
    return execute(commit, new ComposableFuture<>());
  }

  /**
   * Executes a command in the state machine thread and completes the given future asynchronously in the server thread.
   */
  private <T extends Operation<U>, U> CompletableFuture<U> execute(Commit<T> commit, ComposableFuture<U> future) {
    stateExecutor.execute(commit).whenComplete((result, error) -> context.execute(() -> future.accept(result, error)));
    return future;
  }

  /**
   * Executes a method in the state machine thread and completes the given future asynchronously in the server thread.
   */
  private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> supplier) {
    return execute(supplier, new ComposableFuture<>());
  }

  /**
   * Executes a method in the state machine thread and completes the given future asynchronously in the server thread.
   */
  private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> supplier, ComposableFuture<T> future) {
    stateExecutor.execute(() -> {
      supplier.get().whenComplete((result, error) -> {
        context.execute(() -> future.accept(result, error));
      });
    });
    return future;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    if (open)
      return CompletableFuture.completedFuture(null);

    final InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getByName(member.host()), member.port());
    } catch (UnknownHostException e) {
      return Futures.exceptionalFuture(e);
    }

    context = new SingleThreadContext("copycat-server-" + member.id(), serializer);

    openFuture = new CompletableFuture<>();
    context.execute(() -> {

      // Setup the server and connection manager.
      UUID id = UUID.randomUUID();
      server = transport.server(id);
      connections = new ConnectionManager(transport.client(id));

      server.listen(address, this::handleConnect).thenRun(() -> {
        // Open the log.
        log.open();

        // Configure the cluster.
        cluster.configure(0, members, Members.builder().build());

        // Create a state machine executor and configure the state machine.
        stateExecutor = new ServerStateMachineExecutor(new SingleThreadContext("copycat-server-" + member.id() + "-state-%d", serializer.clone()));
        commits = new ServerCommitPool(new ServerCommitCleaner(log), stateExecutor.context().sessions());
        stateMachine.configure(stateExecutor);

        // Transition to the JOIN state. This will cause the server to attempt to join an existing cluster.
        transition(JoinState.class);
        open = true;
      });
    });
    return openFuture.thenRun(() -> LOGGER.info("{} - Started successfully!", member.id()));
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (!open)
      return Futures.exceptionalFuture(new IllegalStateException("context not open"));

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      open = false;

      onStateChange(state -> {
        if (state == RaftServer.State.INACTIVE) {
          server.close().whenCompleteAsync((r1, e1) -> {
            try {
              log.close();
            } catch (Exception e) {
            }

            stateExecutor.close();
            context.close();
            if (e1 != null) {
              future.completeExceptionally(e1);
            } else {
              future.complete(null);
            }
          }, context);
        }
      });

      transition(LeaveState.class);
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
    return CompletableFuture.runAsync(log::delete, context);
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
