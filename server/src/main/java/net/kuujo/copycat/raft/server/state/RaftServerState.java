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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.client.state.RaftClientState;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.server.RaftServer;
import net.kuujo.copycat.raft.server.StateMachine;
import net.kuujo.copycat.transport.Connection;
import net.kuujo.copycat.transport.MessageHandler;
import net.kuujo.copycat.transport.Server;
import net.kuujo.copycat.transport.Transport;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftServerState extends RaftClientState {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftServerState.class);
  private final RaftState stateMachine;
  private final Member member;
  private final Log log;
  private final ClusterState cluster = new ClusterState();
  private final Members members;
  private final Server server;
  private final ConnectionManager connections;
  private final StateConnection stateConnection = new StateConnection();
  private final SessionManager sessions;
  private final Context context;
  private AbstractState state;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;
  private volatile boolean open;

  public RaftServerState(Member member, Members members, Transport transport, Log log, StateMachine stateMachine, Alleycat serializer) {
    super(member.type() == Member.Type.ACTIVE ? members.member(member.id()) : member, members, transport, serializer);

    if (member.type() == Member.Type.ACTIVE) {
      member = members.member(member.id());
      if (member == null) {
        throw new ConfigurationException("active member must be listed in seed members list");
      }
      this.member = member;
    } else if (member.type() == Member.Type.PASSIVE) {
      if (members.member(member.id()) != null) {
        throw new ConfigurationException("passive member cannot be listed in seed members list");
      }
      this.member = member;
    } else {
      throw new ConfigurationException("not a server member type: " + member.type());
    }

    if (member.host() == null) {
      throw new ConfigurationException("member host not configured");
    }
    if (member.port() <= 0) {
      throw new ConfigurationException("member port not configured");
    }

    for (Member server : members.members()) {
      cluster.addMember(new MemberState(server, System.currentTimeMillis()));
    }

    this.context = new SingleThreadContext("copycat-server-" + member.id(), serializer);
    this.log = log;
    this.members = members;
    this.sessions = new SessionManager(member.id(), this);

    this.stateMachine = new RaftState(stateMachine, cluster, sessions, new SingleThreadContext("copycat-server-" + member.id() + "-state-%d", serializer.clone()));

    this.server = transport.server(UUID.randomUUID());
    this.connections = new ConnectionManager(transport.client(UUID.randomUUID()));

    log.compactor().filter(this.stateMachine::filter);
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int getMemberId() {
    return member.id();
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Members getMembers() {
    return members;
  }

  /**
   * Returns the server session manager.
   *
   * @return The server session manager.
   */
  SessionManager getSessionManager() {
    return sessions;
  }

  /**
   * Returns the command serializer.
   *
   * @return The command serializer.
   */
  public Alleycat getSerializer() {
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
  public RaftServerState setElectionTimeout(long electionTimeout) {
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
  public RaftServerState setHeartbeatInterval(long heartbeatInterval) {
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
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout in milliseconds.
   * @return The Raft context.
   */
  public RaftServerState setSessionTimeout(long sessionTimeout) {
    stateMachine.setSessionTimeout(sessionTimeout);
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  RaftServerState setLeader(int leader) {
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
  public int getLeader() {
    return leader;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  RaftServerState setTerm(long term) {
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
  RaftServerState setLastVotedFor(int candidate) {
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
  RaftServerState setCommitIndex(long commitIndex) {
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
  RaftServerState setGlobalIndex(long globalIndex) {
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
   * Returns the current state.
   *
   * @return The current state.
   */
  public RaftServer.State getState() {
    return state.type();
  }

  /**
   * Returns the internal Raft state.
   */
  AbstractState getInternalState() {
    return state;
  }

  /**
   * Returns the state machine proxy.
   *
   * @return The state machine proxy.
   */
  RaftState getStateMachine() {
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
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    context.checkThread();
  }

  @Override
  protected Member selectMember(Query<?> query) {
    if (!query.consistency().isLeaderRequired()) {
      return members.member(member.id());
    }
    return super.selectMember(query);
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
    return CompletableFuture.completedFuture(stateConnection);
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
        this.state = state.getConstructor(RaftServerState.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.getConstructor(RaftServerState.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }
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
    connection.handler(AppendRequest.class, request -> state.append(request));
    connection.handler(PollRequest.class, request -> state.poll(request));
    connection.handler(VoteRequest.class, request -> state.vote(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));
    connection.handler(PublishRequest.class, request -> state.publish(request));
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
        transition(member.type() == Member.Type.ACTIVE ? FollowerState.class : PassiveState.class);
        open = true;
      }).thenCompose(v -> super.open())
        .whenCompleteAsync(future, context);
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
      transition(StartState.class);

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
   * Dummy state connection.
   */
  private class StateConnection implements Connection {

    @Override
    public UUID id() {
      return server.id();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, U> CompletableFuture<U> send(T request) {
      Class<?> clazz = request.getClass();
      if (clazz == RegisterRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.register((RegisterRequest) request));
      } else if (clazz == KeepAliveRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.keepAlive((KeepAliveRequest) request));
      } else if (clazz == AppendRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.append((AppendRequest) request));
      } else if (clazz == PollRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.poll((PollRequest) request));
      } else if (clazz == VoteRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.vote((VoteRequest) request));
      } else if (clazz == CommandRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.command((CommandRequest) request));
      } else if (clazz == QueryRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.query((QueryRequest) request));
      } else if (clazz == PublishRequest.class) {
        return execute(() -> (CompletableFuture<U>) state.publish((PublishRequest) request));
      }
      return Futures.exceptionalFuture(new IllegalStateException("no handlers registered"));
    }

    /**
     * Executes a call to the internal Raft state on the server thread.
     */
    private <U> CompletableFuture<U> execute(Supplier<CompletableFuture<U>> supplier) {
      Context context = Context.currentContext();
      if (context == null) {
        throw new IllegalStateException("not a Copycat thread");
      }

      ComposableFuture<U> future = new ComposableFuture<>();
      RaftServerState.this.context.execute(() -> {
        supplier.get().whenCompleteAsync(future, context);
      });
      return future;
    }

    @Override
    public <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler) {
      return null;
    }

    @Override
    public ListenerContext<Throwable> exceptionListener(Listener<Throwable> listener) {
      return null;
    }

    @Override
    public ListenerContext<Connection> closeListener(Listener<Connection> listener) {
      return null;
    }

    @Override
    public CompletableFuture<Void> close() {
      return null;
    }

  }

}
