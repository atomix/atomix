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
import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.Members;
import net.kuujo.copycat.raft.NoLeaderException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftServerState extends RaftClientState {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftServerState.class);
  private final RaftState stateMachine;
  private final int memberId;
  private final Log log;
  private final ClusterState cluster = new ClusterState();
  private final Members members;
  private final Server server;
  private final ConnectionManager connections;
  private final StateConnection stateConnection = new StateConnection();
  private final SessionManager sessions;
  private final Context context;
  private AbstractState state;
  private ScheduledFuture<?> joinTimer;
  private ScheduledFuture<?> heartbeatTimer;
  private final AtomicBoolean heartbeat = new AtomicBoolean();
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;
  private volatile boolean open;

  public RaftServerState(int memberId, Log log, StateMachine stateMachine, Transport transport, Members members, Alleycat serializer) {
    super(memberId, transport, members, serializer);

    for (Member member : members.members()) {
      cluster.addMember(new MemberState(member.id(), member.type(), System.currentTimeMillis()));
    }

    this.memberId = memberId;
    this.context = new SingleThreadContext("copycat-server-" + memberId, serializer);
    this.log = log;
    this.members = members;
    this.sessions = new SessionManager(memberId, this);

    this.stateMachine = new RaftState(stateMachine, cluster, sessions, new SingleThreadContext("copycat-server-" + memberId + "-state-%d", serializer.clone()));

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
    return memberId;
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
        LOGGER.debug("{} - Found leader {}", memberId, leader);
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", memberId, leader);
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
      LOGGER.debug("{} - Incremented term {}", memberId, term);
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
      LOGGER.debug("{} - Voted for {}", memberId, candidate);
    } else {
      LOGGER.debug("{} - Reset last voted for", memberId);
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
      return members.member(memberId);
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

    LOGGER.info("{} - Transitioning to {}", memberId, state.getSimpleName());

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
   * Joins the cluster.
   */
  private CompletableFuture<Void> join() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      join(100, future);
    });
    return future;
  }

  /**
   * Joins the cluster.
   */
  private CompletableFuture<Void> join(long interval, CompletableFuture<Void> future) {
    join(new ArrayList<>(members.members()), new CompletableFuture<>()).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(null);
      } else {
        long nextInterval = Math.min(interval * 2, 5000);
        joinTimer = context.schedule(() -> join(nextInterval, future), nextInterval, TimeUnit.MILLISECONDS);
      }
    });
    return future;
  }

  /**
   * Joins the cluster by contacting a random member.
   */
  private CompletableFuture<Void> join(List<Member> members, CompletableFuture<Void> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(new NoLeaderException("no leader found"));
      return future;
    }
    return join(selectMember(members), members, future);
  }

  /**
   * Sends a join request to a specific member.
   */
  private CompletableFuture<Void> join(Member member, List<Member> members, CompletableFuture<Void> future) {
    JoinRequest request = JoinRequest.builder()
      .withMember(this.members.member(memberId))
      .build();
    LOGGER.debug("Sending {} to {}", request, member);
    connections.getConnection(member).thenAccept(connection -> {
      connection.<JoinRequest, JoinResponse>send(request).whenComplete((response, error) -> {
        context.checkThread();
        if (error == null && response.status() == Response.Status.OK) {
          setLeader(response.leader());
          setTerm(response.term());
          future.complete(null);
          LOGGER.info("{} - Joined cluster", memberId);
        } else {
          if (member.id() == getLeader()) {
            setLeader(0);
          }
          LOGGER.debug("Cluster join failed, retrying");
          setLeader(0);
          join(members, future);
        }
      });
    });
    return future;
  }

  /**
   * Leaves the cluster.
   */
  private CompletableFuture<Void> leave() {
    return leave(members.members().stream()
      .filter(m -> m.type() == Member.Type.ACTIVE)
      .collect(Collectors.toList()), new CompletableFuture<>());
  }

  /**
   * Leaves the cluster by contacting a random member.
   */
  private CompletableFuture<Void> leave(List<Member> members, CompletableFuture<Void> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(new NoLeaderException("no leader found"));
      return future;
    }
    return leave(selectMember(members), members, future);
  }

  /**
   * Sends a leave request to a specific member.
   */
  private CompletableFuture<Void> leave(Member member, List<Member> members, CompletableFuture<Void> future) {
    LeaveRequest request = LeaveRequest.builder()
      .withMember(this.members.member(memberId))
      .build();
    LOGGER.debug("Sending {} to {}", request, member);
    connections.getConnection(member).thenAccept(connection -> {
      connection.<LeaveRequest, LeaveResponse>send(request).whenComplete((response, error) -> {
        context.checkThread();
        if (error == null && response.status() == Response.Status.OK) {
          future.complete(null);
          LOGGER.info("{} - Left cluster", memberId);
        } else {
          if (member.id() == getLeader()) {
            setLeader(0);
          }
          LOGGER.debug("Cluster leave failed, retrying");
          setLeader(0);
          leave(members, future);
        }
      });
    });
    return future;
  }

  /**
   * Cancels the join timer.
   */
  private void cancelJoinTimer() {
    if (joinTimer != null) {
      LOGGER.debug("cancelling join timer");
      joinTimer.cancel(false);
    }
  }

  /**
   * Cancels the heartbeat timer.
   */
  private void cancelHeartbeatTimer() {
    if (heartbeatTimer != null) {
      LOGGER.debug("cancelling heartbeat timer");
      heartbeatTimer.cancel(false);
    }
  }

  /**
   * Handles a connection.
   */
  private void handleConnect(Connection connection) {
    registerHandlers(connection);
  }

  /**
   * Registers all message handlers.
   */
  private void registerHandlers(Connection connection) {
    context.checkThread();
    sessions.registerConnection(connection);

    connection.handler(JoinRequest.class, state::join);
    connection.handler(LeaveRequest.class, state::leave);
    connection.handler(RegisterRequest.class, state::register);
    connection.handler(KeepAliveRequest.class, state::keepAlive);
    connection.handler(AppendRequest.class, state::append);
    connection.handler(PollRequest.class, state::poll);
    connection.handler(VoteRequest.class, state::vote);
    connection.handler(CommandRequest.class, state::command);
    connection.handler(QueryRequest.class, state::query);
    connection.handler(PublishRequest.class, state::publish);

    connection.closeListener(sessions::unregisterConnection);
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    Member member = members.member(memberId);

    final InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getByName(member.host()), member.port());
    } catch (UnknownHostException e) {
      return Futures.exceptionalFuture(e);
    }

    ComposableFuture<Void> future = new ComposableFuture<>();
    context.execute(() -> {
      if (member.type() == Member.Type.PASSIVE) {
        server.listen(address, this::handleConnect).thenRun(() -> {
          log.open(context);
          transition(PassiveState.class);
        }).thenCompose(v -> join())
          .thenCompose(v -> super.open())
          .thenRun(() -> open = true)
          .whenCompleteAsync(future, context);
      } else {
        server.listen(address, this::handleConnect).thenRun(() -> {
          log.open(context);
          transition(FollowerState.class);
          open = true;
        }).thenCompose(v -> super.open())
          .whenCompleteAsync(future, context);
      }
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
      cancelJoinTimer();
      cancelHeartbeatTimer();
      open = false;
      transition(StartState.class);

      leave().whenCompleteAsync((r1, e1) -> {
        super.close().whenComplete((r2, e2) -> {
          server.close().whenCompleteAsync((r3, e3) -> {
            try {
              log.close();
            } catch (Exception e) {
            }

            if (e1 != null) {
              future.completeExceptionally(e1);
            } else if (e2 != null) {
              future.completeExceptionally(e2);
            } else if (e3 != null) {
              future.completeExceptionally(e3);
            } else {
              future.complete(null);
            }
          }, context);
        });
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
      if (clazz == JoinRequest.class) {
        return (CompletableFuture<U>) state.join((JoinRequest) request);
      } else if (clazz == LeaveRequest.class) {
        return (CompletableFuture<U>) state.leave((LeaveRequest) request);
      } else if (clazz == RegisterRequest.class) {
        return (CompletableFuture<U>) state.register((RegisterRequest) request);
      } else if (clazz == KeepAliveRequest.class) {
        return (CompletableFuture<U>) state.keepAlive((KeepAliveRequest) request);
      } else if (clazz == AppendRequest.class) {
        return (CompletableFuture<U>) state.append((AppendRequest) request);
      } else if (clazz == PollRequest.class) {
        return (CompletableFuture<U>) state.poll((PollRequest) request);
      } else if (clazz == VoteRequest.class) {
        return (CompletableFuture<U>) state.vote((VoteRequest) request);
      } else if (clazz == CommandRequest.class) {
        return (CompletableFuture<U>) state.command((CommandRequest) request);
      } else if (clazz == QueryRequest.class) {
        return (CompletableFuture<U>) state.query((QueryRequest) request);
      } else if (clazz == PublishRequest.class) {
        return (CompletableFuture<U>) state.publish((PublishRequest) request);
      }
      return Futures.exceptionalFuture(new IllegalStateException("no handlers registered"));
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
