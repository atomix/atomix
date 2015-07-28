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

import net.kuujo.copycat.*;
import net.kuujo.copycat.raft.client.state.ClientContext;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.log.Compaction;
import net.kuujo.copycat.raft.log.Entry;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.RaftServer;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.raft.server.log.*;
import net.kuujo.copycat.raft.transport.Connection;
import net.kuujo.copycat.raft.transport.LocalConnection;
import net.kuujo.copycat.raft.transport.Server;
import net.kuujo.copycat.raft.transport.Transport;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerContext extends ClientContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerContext.class);
  private final Listeners<RaftServer.State> listeners = new Listeners<>();
  private final Context context;
  private final StateMachine stateMachine;
  private final Context stateContext;
  private final Member member;
  private final Log log;
  private final ClusterState cluster;
  private final Members members;
  private final Server server;
  private final ConnectionManager connections;
  private final LocalConnection inbound;
  private final LocalConnection outbound;
  private final SessionManager sessions;
  private AbstractState state;
  private final Map<Long, SessionContext> contexts = new HashMap<>();
  private final Map<Long, List<Runnable>> queries = new HashMap<>();
  private long electionTimeout = 500;
  private long sessionTimeout = 5000;
  private long heartbeatInterval = 250;
  private long lastApplied;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;
  private volatile boolean open;

  public ServerContext(int memberId, Members members, Transport transport, Log log, StateMachine stateMachine, Serializer serializer) {
    super(members, transport, serializer);

    member = members.member(memberId);
    if (member == null) {
      throw new ConfigurationException("active member must be listed in members list");
    }

    if (member.host() == null) {
      throw new ConfigurationException("member host not configured");
    }
    if (member.port() <= 0) {
      throw new ConfigurationException("member port not configured");
    }

    this.cluster = new ClusterState(this, member);
    this.members = members;

    this.context = new SingleThreadContext("copycat-server-" + member.id(), serializer);
    this.log = log;
    this.sessions = new SessionManager();
    this.stateMachine = stateMachine;
    this.stateContext = new SingleThreadContext("copycat-server-" + member.id() + "-state-%d", serializer.clone());
    this.server = transport.server(UUID.randomUUID());
    this.connections = new ConnectionManager(transport.client(UUID.randomUUID()));
    this.inbound = new LocalConnection(id, getContext());
    this.outbound = new LocalConnection(id, super.getContext());
    inbound.connect(outbound);
    outbound.connect(inbound);

    log.compactor().filter(this::filter);
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
    log.compactor().setMinorCompactionIndex(commitIndex);
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
    log.compactor().setMajorCompactionIndex(globalIndex);
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

    List<Runnable> queries = this.queries.remove(lastApplied);

    if (queries != null) {
      queries.forEach(Runnable::run);
    }
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

  @Override
  protected Member selectMember() {
    return member;
  }

  @Override
  protected CompletableFuture<Void> register(List<Member> members) {
    return register(members, new CompletableFuture<>()).thenAccept(response -> {
      setSessionId(response.session());
    });
  }

  @Override
  protected CompletableFuture<Void> keepAlive(List<Member> members) {
    return keepAlive(members, new CompletableFuture<>()).thenAccept(response -> {
      setVersion(response.version());
    });
  }

  /**
   * This method always returns a connection to the local state machine for servers.
   */
  @Override
  protected CompletableFuture<Connection> getConnection(Member member) {
    return CompletableFuture.completedFuture(outbound);
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
    sessions.registerConnection(connection);
    registerHandlers(connection);
    connection.closeListener(sessions::unregisterConnection);
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
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A boolean value indicating whether to keep the entry.
   */
  CompletableFuture<Boolean> filter(Entry entry, Compaction compaction) {
    if (entry instanceof CommandEntry) {
      return filter((CommandEntry) entry, compaction);
    } else if (entry instanceof KeepAliveEntry) {
      return filter((KeepAliveEntry) entry, compaction);
    } else if (entry instanceof RegisterEntry) {
      return filter((RegisterEntry) entry, compaction);
    } else if (entry instanceof ConfigurationEntry) {
      return filter((ConfigurationEntry) entry, compaction);
    } else if (entry instanceof NoOpEntry) {
      return filter((NoOpEntry) entry, compaction);
    }
    return CompletableFuture.completedFuture(false);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @param compaction The compaction process.
   * @return A boolean value indicating whether to keep the entry.
   */
  CompletableFuture<Boolean> filter(RegisterEntry entry, Compaction compaction) {
    return Futures.completedFuture(contexts.containsKey(entry.getIndex()));
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @param compaction The compaction process.
   * @return A boolean value indicating whether to keep the entry.
   */
  CompletableFuture<Boolean> filter(KeepAliveEntry entry, Compaction compaction) {
    return Futures.completedFuture(contexts.containsKey(entry.getIndex()) && contexts.get(entry.getIndex()).index == entry
      .getIndex());
  }

  /**
   * Filters a configuration entry.
   *
   * @param entry The entry to filter.
   * @param compaction The compaction process.
   * @return Indicates whether to keep the entry.
   */
  CompletableFuture<Boolean> filter(ConfigurationEntry entry, Compaction compaction) {
    return Futures.completedFuture(entry.getIndex() == cluster.getVersion() || entry.getIndex() >= lastApplied);
  }

  /**
   * Filters a no-op entry.
   *
   * @param entry The entry to filter.
   * @param compaction The compaction process.
   * @return A boolean value indicating whether to keep the entry.
   */
  CompletableFuture<Boolean> filter(NoOpEntry entry, Compaction compaction) {
    return Futures.completedFuture(false);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @param compaction The compaction process.
   * @return A boolean value indicating whether to keep the entry.
   */
  CompletableFuture<Boolean> filter(CommandEntry entry, Compaction compaction) {
    Commit<? extends Command> commit = new Commit<>(entry.getIndex(), null, entry.getTimestamp(), entry.getCommand());
    return execute(() -> stateMachine.filter(commit, compaction));
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
    return register(entry.getIndex(), entry.getConnection(), entry.getTimestamp());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   */
  CompletableFuture<Void> apply(KeepAliveEntry entry) {
    return keepAlive(entry.getIndex(), entry.getTimestamp(), entry.getSession());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Object> apply(CommandEntry entry) {
    return command(entry.getIndex(), entry.getSession(), entry.getRequest(), entry.getResponse(), entry.getTimestamp(), entry.getCommand());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Object> apply(QueryEntry entry) {
    return query(entry.getIndex(), entry.getSession(), entry.getVersion(), entry.getTimestamp(), entry.getQuery());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Members> apply(ConfigurationEntry entry) {
    return configure(entry.getIndex(), entry.getActive(), entry.getPassive());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Long> apply(NoOpEntry entry) {
    return noop(entry.getIndex());
  }

  /**
   * Registers a member session.
   *
   * @param index The registration index.
   * @param connectionId the session connection ID.
   * @param timestamp The registration timestamp.
   * @return The session ID.
   */
  private CompletableFuture<Long> register(long index, UUID connectionId, long timestamp) {
    Session session = sessions.registerSession(index, connectionId);

    SessionContext context = new SessionContext(timestamp);
    contexts.put(session.id(), context);

    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Long> future = new ComposableFuture<>();
    stateContext.execute(() -> {
      stateMachine.register(session);
      this.context.execute(() -> future.complete(index));
    });

    setLastApplied(session.id());
    return future;
  }

  /**
   * Keeps a member session alive.
   *
   * @param index The keep alive index.
   * @param timestamp The keep alive timestamp.
   * @param sessionId The session to keep alive.
   */
  private CompletableFuture<Void> keepAlive(long index, long timestamp, long sessionId) {
    SessionContext context = contexts.get(sessionId);

    CompletableFuture<Void> future;
    if (context == null) {
      LOGGER.warn("Unknown session: " + sessionId);
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + sessionId));
    } else if (!context.update(index, timestamp)) {
      LOGGER.warn("Expired session: " + sessionId);
      future = expireSession(sessionId);
    } else {
      future = Futures.completedFutureAsync(null, this.context);
    }

    setLastApplied(index);
    return future;
  }

  /**
   * Applies a no-op to the state machine.
   *
   * @param index The no-op index.
   * @return The no-op index.
   */
  private CompletableFuture<Long> noop(long index) {
    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    setLastApplied(index);
    return Futures.completedFuture(index);
  }

  /**
   * Applies a configuration entry to the state machine.
   *
   * @param version The entry version.
   * @param active The active members.
   * @param passive The passive members.
   * @return A completable future to be completed with the active member configuration.
   */
  private CompletableFuture<Members> configure(long version, Members active, Members passive) {
    if (cluster.isPassive()) {
      cluster.configure(version, active, passive);
      if (cluster.isActive()) {
        transition(FollowerState.class);
      }
    } else {
      cluster.configure(version, active, passive);
      if (cluster.isPassive()) {
        transition(PassiveState.class);
      }
    }
    return Futures.completedFuture(cluster.buildActiveMembers());
  }

  /**
   * Applies a command to the state machine.
   *
   * @param index The command index.
   * @param sessionId The command session ID.
   * @param request The command request ID.
   * @param response The command response ID.
   * @param timestamp The command timestamp.
   * @param command The command to apply.
   * @return The command result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> command(long index, long sessionId, long request, long response, long timestamp, Command command) {
    final CompletableFuture<Object> future;

    // First check to ensure that the session exists.
    SessionContext context = contexts.get(sessionId);
    if (context == null) {
      LOGGER.warn("Unknown session: " + sessionId);
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session " + sessionId));
    } else if (!context.update(index, timestamp)) {
      LOGGER.warn("Expired session: " + sessionId);
      future = expireSession(sessionId);
    } else if (context.responses.containsKey(request)) {
      future = CompletableFuture.completedFuture(context.responses.get(request));
    } else {
      // Apply the command to the state machine.
      Session session = sessions.getSession(sessionId);

      future = execute(() -> stateMachine.apply(new Commit(index, session, timestamp, command)))
        .thenApply(result -> {
          // Store the command result in the session.
          context.responses.put(request, result);

          // Clear any responses that have been received by the client for the session.
          context.responses.headMap(response, true).clear();
          return result;
        });
    }

    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    setLastApplied(index);

    return future;
  }

  /**
   * Applies a query to the state machine.
   *
   * @param index The query index.
   * @param sessionId The query session ID.
   * @param version The request version.
   * @param timestamp The query timestamp.
   * @param query The query to apply.
   * @return The query result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> query(long index, long sessionId, long version, long timestamp, Query query) {
    // If the session has not yet been opened or if the client provided a version greater than the last applied index
    // then wait until the up-to-date index is applied to the state machine.
    if (sessionId > lastApplied || version > lastApplied) {
      ComposableFuture<Object> future = new ComposableFuture<>();
      Session session = sessions.getSession(sessionId);

      List<Runnable> queries = this.queries.computeIfAbsent(Math.max(sessionId, version), id -> new ArrayList<>());
      queries.add(() -> {
        execute(() -> stateMachine.apply(new Commit(index, session, timestamp, query)), future);
      });

      return future;
    } else {

      // Verify that the client's session is still alive.
      SessionContext context = contexts.get(sessionId);
      if (context == null) {
        LOGGER.warn("Unknown session: " + sessionId);
        return Futures.exceptionalFuture(new UnknownSessionException("unknown session " + sessionId));
      } else if (!context.expire(timestamp)) {
        LOGGER.warn("Expired session: " + sessionId);
        return expireSession(sessionId);
      } else {
        Session session = sessions.getSession(sessionId);
        return execute(() -> stateMachine.apply(new Commit(index, session, timestamp, query)));
      }
    }
  }

  /**
   * Expires a session.
   */
  private <T> CompletableFuture<T> expireSession(long sessionId) {
    CompletableFuture<T> future = new CompletableFuture<>();
    contexts.remove(sessionId);
    ServerSession session = sessions.unregisterSession(sessionId);
    if (session != null) {
      stateContext.execute(() -> {
        session.expire();
        stateMachine.expire(session);
        context.execute(() -> future.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId)));
      });
    } else {
      future.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId));
    }
    return future;
  }

  /**
   * Executes a method in the state machine thread and completes the given future asynchronously in the same thread.
   */
  private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> supplier) {
    return execute(supplier, new ComposableFuture<>());
  }

  /**
   * Executes a method in the state machine thread and completes the given future asynchronously in the same thread.
   */
  private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> supplier, ComposableFuture<T> future) {
    stateContext.execute(() -> {
      supplier.get().whenCompleteAsync(future, context);
    });
    return future;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    final InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getByName(member.host()), member.port());
    } catch (UnknownHostException e) {
      return Futures.exceptionalFuture(e);
    }

    ComposableFuture<Void> future = new ComposableFuture<>();
    context.execute(() -> {
      server.listen(address, this::handleConnect).thenRun(() -> {
        log.open(context);
        cluster.configure(0, members, Members.builder().build());

        onStateChange(state -> {
          if (state == RaftServer.State.FOLLOWER) {
            super.open().whenCompleteAsync(future, context);
          }
        });

        // Register handlers on the inbound local connection.
        handleConnect(inbound);

        // Set up client handlers.
        connect(outbound);

        transition(JoinState.class);
        open = true;
      });
    });
    return future;
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
          super.close().whenCompleteAsync((r1, e1) -> {
            server.close().whenCompleteAsync((r2, e2) -> {
              try {
                log.close();
              } catch (Exception e) {
              }

              if (e1 != null) {
                future.completeExceptionally(e1);
              } else if (e2 != null) {
                future.completeExceptionally(e2);
              } else {
                future.complete(null);
              }
            }, context);
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

  /**
   * Session context.
   */
  private class SessionContext {
    private long index;
    private long timestamp;
    private final TreeMap<Long, Object> responses = new TreeMap<>();

    private SessionContext(long timestamp) {
      this.timestamp = timestamp;
    }

    /**
     * Returns the session timestamp.
     *
     * @return The session timestamp.
     */
    public long timestamp() {
      return timestamp;
    }

    /**
     * Updates the session.
     *
     * @param timestamp The session.
     */
    private boolean expire(long timestamp) {
      if (timestamp - sessionTimeout > this.timestamp) {
        return false;
      }
      this.timestamp = timestamp;
      return true;
    }

    /**
     * Updates the session.
     *
     * @param timestamp The session.
     */
    private boolean update(long index, long timestamp) {
      if (timestamp - sessionTimeout > this.timestamp) {
        return false;
      }
      this.index = index;
      this.timestamp = timestamp;
      return true;
    }
  }

}
