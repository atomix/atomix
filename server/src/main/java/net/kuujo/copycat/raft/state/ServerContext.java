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

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderTypeResolver;
import net.kuujo.copycat.io.storage.Log;
import net.kuujo.copycat.io.storage.Storage;
import net.kuujo.copycat.io.transport.Address;
import net.kuujo.copycat.io.transport.Connection;
import net.kuujo.copycat.io.transport.Server;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.protocol.request.*;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.Listener;
import net.kuujo.copycat.util.Listeners;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

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
  private final StateMachine userStateMachine;
  private final Address address;
  private final Storage storage;
  private final ClusterState cluster;
  private final Map<Integer, Address> members;
  private final Transport transport;
  private Log log;
  private ServerStateMachine stateMachine;
  private Server server;
  private ConnectionManager connections;
  private AbstractState state = new InactiveState(this);
  private Duration electionTimeout = Duration.ofMillis(500);
  private Duration sessionTimeout = Duration.ofMillis(5000);
  private Duration heartbeatInterval = Duration.ofMillis(150);
  private int leader;
  private long term;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;
  private volatile boolean open;
  private volatile CompletableFuture<Void> openFuture;

  public ServerContext(Address address, Collection<Address> members, Transport transport, Storage storage, StateMachine stateMachine, Serializer serializer) {
    this.address = Assert.notNull(address, "address");
    this.members = new HashMap<>();
    members.forEach(m -> this.members.put(m.hashCode(), m));
    this.members.put(address.hashCode(), address);
    this.transport = Assert.notNull(transport, "transport");
    this.cluster = new ClusterState(this, address);
    this.serializer = Assert.notNull(serializer, "serializer");

    storage.serializer().resolve(new ServiceLoaderTypeResolver());
    serializer.resolve(new ServiceLoaderTypeResolver());

    this.storage = Assert.notNull(storage, "storage");
    this.userStateMachine = Assert.notNull(stateMachine, "stateMachine");
  }

  /**
   * Registers a state change listener.
   *
   * @param listener The state change listener.
   * @return The listener context.
   */
  public Listener<RaftServer.State> onStateChange(Consumer<RaftServer.State> listener) {
    return listeners.add(listener);
  }

  /**
   * Returns the server address.
   *
   * @return The server address.
   */
  public Address getAddress() {
    return address;
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
  public ServerContext setElectionTimeout(Duration electionTimeout) {
    this.electionTimeout = electionTimeout;
    return this;
  }

  /**
   * Returns the election timeout.
   *
   * @return The election timeout.
   */
  public Duration getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @return The Raft context.
   */
  public ServerContext setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
    return this;
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return The heartbeat interval in milliseconds.
   */
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public Duration getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout.
   * @return The Raft state machine.
   */
  public ServerContext setSessionTimeout(Duration sessionTimeout) {
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
        Address address = members.get(leader);
        if (address == null)
          throw new IllegalStateException("unknown leader: " + leader);
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", this.address, address);
        if (openFuture != null) {
          openFuture.complete(null);
          openFuture = null;
        }
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        Address address = members.get(leader);
        if (address == null)
          throw new IllegalStateException("unknown leader: " + leader);
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", this.address, address);
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
  public Address getLeader() {
    if (leader == 0) {
      return null;
    } else if (leader == address.hashCode()) {
      return address;
    }

    MemberState member = cluster.getMember(leader);
    return member != null ? member.getAddress() : null;
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
      LOGGER.debug("{} - Incremented term {}", address, term);
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
    Address address = members.get(candidate);
    if (address == null) {
      throw new IllegalStateException("unknown candidate: " + candidate);
    }

    this.lastVotedFor = candidate;

    if (candidate != 0) {
      LOGGER.debug("{} - Voted for {}", this.address, address);
    } else {
      LOGGER.debug("{} - Reset last voted for", this.address);
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
    log.commit(this.globalIndex);
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
   * Returns the server state machine.
   *
   * @return The server state machine.
   */
  ServerStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Returns the last index applied to the state machine.
   *
   * @return The last index applied to the state machine.
   */
  public long getLastApplied() {
    return stateMachine.getLastApplied();
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

    LOGGER.info("{} - Transitioning to {}", address, state.getSimpleName());

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
    stateMachine.executor().context().sessions().registerConnection(connection);
    registerHandlers(connection);
    connection.closeListener(stateMachine.executor().context().sessions()::unregisterConnection);
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

  @Override
  public synchronized CompletableFuture<Void> open() {
    if (open)
      return CompletableFuture.completedFuture(null);

    context = new SingleThreadContext("copycat-server-" + address, serializer);

    openFuture = new CompletableFuture<>();
    context.executor().execute(() -> {

      // Open the log.
      log = storage.open();

      // Configure the cluster.
      cluster.configure(0, members.values(), Collections.EMPTY_LIST);

      // Create a state machine executor and configure the state machine.
      Context stateContext = new SingleThreadContext("copycat-server-" + address + "-state-%d", serializer.clone());
      stateMachine = new ServerStateMachine(userStateMachine, log::clean, stateContext);

      // Setup the server and connection manager.
      UUID id = UUID.randomUUID();
      server = transport.server(id);
      connections = new ConnectionManager(transport.client(id));

      server.listen(address, this::handleConnect).thenRun(() -> {
        // Transition to the JOIN state. This will cause the server to attempt to join an existing cluster.
        transition(JoinState.class);
        open = true;
      });
    });
    return openFuture.thenRun(() -> LOGGER.info("{} - Started successfully!", address));
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
    context.executor().execute(() -> {
      open = false;

      onStateChange(state -> {
        if (state == RaftServer.State.INACTIVE) {
          server.close().whenCompleteAsync((r1, e1) -> {
            try {
              log.close();
            } catch (Exception e) {
            }

            stateMachine.close();
            context.close();
            if (e1 != null) {
              future.completeExceptionally(e1);
            } else {
              future.complete(null);
            }
          }, context.executor());
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
    return CompletableFuture.runAsync(log::delete, context.executor());
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
