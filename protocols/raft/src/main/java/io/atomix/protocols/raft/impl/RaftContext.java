/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.impl;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftClusterContext;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.protocol.TransferRequest;
import io.atomix.protocols.raft.roles.ActiveRole;
import io.atomix.protocols.raft.roles.CandidateRole;
import io.atomix.protocols.raft.roles.FollowerRole;
import io.atomix.protocols.raft.roles.InactiveRole;
import io.atomix.protocols.raft.roles.LeaderRole;
import io.atomix.protocols.raft.roles.PassiveRole;
import io.atomix.protocols.raft.roles.PromotableRole;
import io.atomix.protocols.raft.roles.RaftRole;
import io.atomix.protocols.raft.session.RaftSessionRegistry;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.protocols.raft.storage.log.RaftLog;
import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.storage.log.RaftLogWriter;
import io.atomix.protocols.raft.storage.snapshot.SnapshotStore;
import io.atomix.protocols.raft.storage.system.MetaStore;
import io.atomix.protocols.raft.utils.LoadMonitor;
import io.atomix.storage.StorageException;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Manages the volatile state and state transitions of a Raft server.
 * <p>
 * This class is the primary vehicle for managing the state of a server. All state that is shared across roles (i.e. follower, candidate, leader)
 * is stored in the cluster state. This includes Raft-specific state like the current leader and term, the log, and the cluster configuration.
 */
public class RaftContext implements AutoCloseable {
  private static final int LOAD_WINDOW_SIZE = 5;
  private static final int HIGH_LOAD_THRESHOLD = 500;

  private final Logger log;
  private final Set<Consumer<RaftServer.Role>> roleChangeListeners = new CopyOnWriteArraySet<>();
  private final Set<Consumer<State>> stateChangeListeners = new CopyOnWriteArraySet<>();
  private final Set<Consumer<RaftMember>> electionListeners = new CopyOnWriteArraySet<>();
  protected final String name;
  protected final ThreadContext threadContext;
  protected final PrimitiveTypeRegistry primitiveTypes;
  protected final ClusterMembershipService membershipService;
  protected final RaftClusterContext cluster;
  protected final RaftServerProtocol protocol;
  protected final RaftStorage storage;
  protected final RaftServiceRegistry services = new RaftServiceRegistry();
  protected final RaftSessionRegistry sessions = new RaftSessionRegistry();
  private final LoadMonitor loadMonitor;
  private volatile State state = State.ACTIVE;
  private final MetaStore meta;
  private final RaftLog raftLog;
  private final RaftLogWriter logWriter;
  private final RaftLogReader logReader;
  private final SnapshotStore snapshotStore;
  private final RaftServiceManager stateMachine;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext loadContext;
  private final ThreadContext stateContext;
  private final boolean closeOnStop;
  private RaftRole role = new InactiveRole(this);
  private Duration electionTimeout = Duration.ofMillis(500);
  private Duration heartbeatInterval = Duration.ofMillis(150);
  private Duration sessionTimeout = Duration.ofMillis(5000);
  private volatile MemberId leader;
  private volatile long term;
  private MemberId lastVotedFor;
  private long commitIndex;
  private volatile long firstCommitIndex;
  private volatile long lastApplied;

  @SuppressWarnings("unchecked")
  public RaftContext(
      String name,
      MemberId localMemberId,
      ClusterMembershipService membershipService,
      RaftServerProtocol protocol,
      RaftStorage storage,
      PrimitiveTypeRegistry primitiveTypes,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop) {
    this.name = checkNotNull(name, "name cannot be null");
    this.membershipService = checkNotNull(membershipService, "membershipService cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.storage = checkNotNull(storage, "storage cannot be null");
    this.primitiveTypes = checkNotNull(primitiveTypes, "registry cannot be null");
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftServer.class)
        .addValue(name)
        .build());

    // Lock the storage directory.
    if (!storage.lock(localMemberId.id())) {
      throw new StorageException("Failed to acquire storage lock; ensure each Raft server is configured with a distinct storage directory");
    }

    String baseThreadName = String.format("raft-server-%s", name);
    this.threadContext = new SingleThreadContext(namedThreads(baseThreadName, log));
    this.loadContext = new SingleThreadContext(namedThreads(baseThreadName + "-load", log));
    this.stateContext = new SingleThreadContext(namedThreads(baseThreadName + "-state", log));

    this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
    this.closeOnStop = closeOnStop;

    this.loadMonitor = new LoadMonitor(LOAD_WINDOW_SIZE, HIGH_LOAD_THRESHOLD, loadContext);

    // Open the metadata store.
    this.meta = storage.openMetaStore();

    // Load the current term and last vote from disk.
    this.term = meta.loadTerm();
    this.lastVotedFor = meta.loadVote();

    // Construct the core log, reader, writer, and compactor.
    this.raftLog = storage.openLog();
    this.logWriter = raftLog.writer();
    this.logReader = raftLog.openReader(1, RaftLogReader.Mode.ALL);

    // Open the snapshot store.
    this.snapshotStore = storage.openSnapshotStore();

    // Create a new internal server state machine.
    this.stateMachine = new RaftServiceManager(this, stateContext, threadContextFactory);

    this.cluster = new RaftClusterContext(localMemberId, this);

    // Register protocol listeners.
    registerHandlers(protocol);
  }

  public MemberId localMemberId() {
    return membershipService.getLocalMember().id();
  }

  /**
   * Returns the server name.
   *
   * @return The server name.
   */
  public String getName() {
    return name;
  }

  /**
   * Adds a role change listener.
   *
   * @param listener The role change listener.
   */
  public void addRoleChangeListener(Consumer<RaftServer.Role> listener) {
    roleChangeListeners.add(listener);
  }

  /**
   * Removes a role change listener.
   *
   * @param listener The role change listener.
   */
  public void removeRoleChangeListener(Consumer<RaftServer.Role> listener) {
    roleChangeListeners.remove(listener);
  }

  /**
   * Adds a state change listener.
   *
   * @param listener The state change listener.
   */
  public void addStateChangeListener(Consumer<State> listener) {
    stateChangeListeners.add(listener);
  }

  /**
   * Removes a state change listener.
   *
   * @param listener The state change listener.
   */
  public void removeStateChangeListener(Consumer<State> listener) {
    stateChangeListeners.remove(listener);
  }

  /**
   * Awaits a state change.
   *
   * @param state    the state for which to wait
   * @param listener the listener to call when the next state change occurs
   */
  public void awaitState(State state, Consumer<State> listener) {
    if (this.state == state) {
      listener.accept(this.state);
    } else {
      addStateChangeListener(new Consumer<State>() {
        @Override
        public void accept(State state) {
          listener.accept(state);
          removeStateChangeListener(this);
        }
      });
    }
  }

  /**
   * Adds a leader election listener.
   *
   * @param listener The leader election listener.
   */
  public void addLeaderElectionListener(Consumer<RaftMember> listener) {
    electionListeners.add(listener);
  }

  /**
   * Removes a leader election listener.
   *
   * @param listener The leader election listener.
   */
  public void removeLeaderElectionListener(Consumer<RaftMember> listener) {
    electionListeners.remove(listener);
  }

  /**
   * Returns the execution context.
   *
   * @return The execution context.
   */
  public ThreadContext getThreadContext() {
    return threadContext;
  }

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  /**
   * Returns the server protocol.
   *
   * @return The server protocol.
   */
  public RaftServerProtocol getProtocol() {
    return protocol;
  }

  /**
   * Returns the server storage.
   *
   * @return The server storage.
   */
  public RaftStorage getStorage() {
    return storage;
  }

  /**
   * Returns the current server state.
   *
   * @return the current server state
   */
  public State getState() {
    return state;
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout The election timeout.
   */
  public void setElectionTimeout(Duration electionTimeout) {
    this.electionTimeout = electionTimeout;
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
   * @param heartbeatInterval The Raft heartbeat interval.
   */
  public void setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = checkNotNull(heartbeatInterval, "heartbeatInterval cannot be null");
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return The heartbeat interval.
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
   */
  public void setSessionTimeout(Duration sessionTimeout) {
    this.sessionTimeout = checkNotNull(sessionTimeout, "sessionTimeout cannot be null");
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   */
  public void setLeader(MemberId leader) {
    if (!Objects.equals(this.leader, leader)) {
      if (leader == null) {
        this.leader = null;
      } else {
        // If a valid leader ID was specified, it must be a member that's currently a member of the
        // ACTIVE members configuration. Note that we don't throw exceptions for unknown members. It's
        // possible that a failure following a configuration change could result in an unknown leader
        // sending AppendRequest to this server. Simply configure the leader if it's known.
        DefaultRaftMember member = cluster.getMember(leader);
        if (member != null) {
          this.leader = leader;
          log.info("Found leader {}", member.memberId());
          electionListeners.forEach(l -> l.accept(member));
        }
      }

      this.lastVotedFor = null;
      meta.storeVote(null);
    }
  }

  /**
   * Returns the cluster state.
   *
   * @return The cluster state.
   */
  public RaftClusterContext getCluster() {
    return cluster;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  public DefaultRaftMember getLeader() {
    // Store in a local variable to prevent race conditions and/or multiple volatile lookups.
    MemberId leader = this.leader;
    return leader != null ? cluster.getMember(leader) : null;
  }

  /**
   * Returns a boolean indicating whether this server is the current leader.
   *
   * @return Indicates whether this server is the leader.
   */
  public boolean isLeader() {
    MemberId leader = this.leader;
    return leader != null && leader.equals(cluster.getMember().memberId());
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   */
  public void setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = null;
      this.lastVotedFor = null;
      meta.storeTerm(this.term);
      meta.storeVote(this.lastVotedFor);
      log.debug("Set term {}", term);
    }
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
   */
  public void setLastVotedFor(MemberId candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    checkState(!(lastVotedFor != null && candidate != null), "Already voted for another candidate");
    DefaultRaftMember member = cluster.getMember(candidate);
    checkState(member != null, "Unknown candidate: %d", candidate);
    this.lastVotedFor = candidate;
    meta.storeVote(this.lastVotedFor);

    if (candidate != null) {
      log.debug("Voted for {}", member.memberId());
    } else {
      log.trace("Reset last voted for");
    }
  }

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  public MemberId getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return the previous commit index
   */
  public long setCommitIndex(long commitIndex) {
    checkArgument(commitIndex >= 0, "commitIndex must be positive");
    long previousCommitIndex = this.commitIndex;
    if (commitIndex > previousCommitIndex) {
      this.commitIndex = commitIndex;
      logWriter.commit(Math.min(commitIndex, logWriter.getLastIndex()));
      long configurationIndex = cluster.getConfiguration().index();
      if (configurationIndex > previousCommitIndex && configurationIndex <= commitIndex) {
        cluster.commit();
      }
      setFirstCommitIndex(commitIndex);
    }
    return previousCommitIndex;
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
   * Sets the first commit index.
   *
   * @param firstCommitIndex The first commit index.
   */
  public void setFirstCommitIndex(long firstCommitIndex) {
    if (this.firstCommitIndex == 0) {
      this.firstCommitIndex = firstCommitIndex;
    }
  }

  /**
   * Returns the first commit index.
   *
   * @return The first commit index.
   */
  public long getFirstCommitIndex() {
    return firstCommitIndex;
  }

  /**
   * Sets the last applied index.
   *
   * @param lastApplied the last applied index
   */
  public void setLastApplied(long lastApplied) {
    this.lastApplied = Math.max(this.lastApplied, lastApplied);
    if (state == State.ACTIVE) {
      threadContext.execute(() -> {
        if (state == State.ACTIVE && this.lastApplied >= firstCommitIndex) {
          state = State.READY;
          stateChangeListeners.forEach(l -> l.accept(state));
        }
      });
    }
  }

  /**
   * Returns the last applied index.
   *
   * @return the last applied index
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Returns the server load monitor.
   *
   * @return the server load monitor
   */
  public LoadMonitor getLoadMonitor() {
    return loadMonitor;
  }

  /**
   * Returns the server state machine.
   *
   * @return The server state machine.
   */
  public RaftServiceManager getServiceManager() {
    return stateMachine;
  }

  /**
   * Returns the server service registry.
   *
   * @return the server service registry
   */
  public RaftServiceRegistry getServices() {
    return services;
  }

  /**
   * Returns the server session registry.
   *
   * @return the server session registry
   */
  public RaftSessionRegistry getSessions() {
    return sessions;
  }

  /**
   * Returns the server state machine registry.
   *
   * @return The server state machine registry.
   */
  public PrimitiveTypeRegistry getPrimitiveTypes() {
    return primitiveTypes;
  }

  /**
   * Returns the current server role.
   *
   * @return The current server role.
   */
  public RaftServer.Role getRole() {
    return role.role();
  }

  /**
   * Returns the current server state.
   *
   * @return The current server state.
   */
  public RaftRole getRaftRole() {
    return role;
  }

  /**
   * Returns the server metadata store.
   *
   * @return The server metadata store.
   */
  public MetaStore getMetaStore() {
    return meta;
  }

  /**
   * Returns the server log.
   *
   * @return The server log.
   */
  public RaftLog getLog() {
    return raftLog;
  }

  /**
   * Returns the server log writer.
   *
   * @return The log writer.
   */
  public RaftLogWriter getLogWriter() {
    return logWriter;
  }

  /**
   * Returns the server log reader.
   *
   * @return The log reader.
   */
  public RaftLogReader getLogReader() {
    return logReader;
  }

  /**
   * Returns the server snapshot store.
   *
   * @return The server snapshot store.
   */
  public SnapshotStore getSnapshotStore() {
    return snapshotStore;
  }

  /**
   * Compacts the server logs.
   *
   * @return a future to be completed once the logs have been compacted
   */
  public CompletableFuture<Void> compact() {
    ComposableFuture<Void> future = new ComposableFuture<>();
    threadContext.execute(() -> stateMachine.compact().whenComplete(future));
    return future;
  }

  /**
   * Checks that the current thread is the state context thread.
   */
  public void checkThread() {
    threadContext.checkThread();
  }

  /**
   * Registers server handlers on the configured protocol.
   */
  private void registerHandlers(RaftServerProtocol protocol) {
    protocol.registerOpenSessionHandler(request -> runOnContextIfReady(() -> role.onOpenSession(request), OpenSessionResponse::builder));
    protocol.registerCloseSessionHandler(request -> runOnContextIfReady(() -> role.onCloseSession(request), CloseSessionResponse::builder));
    protocol.registerKeepAliveHandler(request -> runOnContextIfReady(() -> role.onKeepAlive(request), KeepAliveResponse::builder));
    protocol.registerMetadataHandler(request -> runOnContextIfReady(() -> role.onMetadata(request), MetadataResponse::builder));
    protocol.registerConfigureHandler(request -> runOnContext(() -> role.onConfigure(request)));
    protocol.registerInstallHandler(request -> runOnContext(() -> role.onInstall(request)));
    protocol.registerJoinHandler(request -> runOnContext(() -> role.onJoin(request)));
    protocol.registerReconfigureHandler(request -> runOnContext(() -> role.onReconfigure(request)));
    protocol.registerLeaveHandler(request -> runOnContext(() -> role.onLeave(request)));
    protocol.registerTransferHandler(request -> runOnContext(() -> role.onTransfer(request)));
    protocol.registerAppendHandler(request -> runOnContext(() -> role.onAppend(request)));
    protocol.registerPollHandler(request -> runOnContext(() -> role.onPoll(request)));
    protocol.registerVoteHandler(request -> runOnContext(() -> role.onVote(request)));
    protocol.registerCommandHandler(request -> runOnContextIfReady(() -> role.onCommand(request), CommandResponse::builder));
    protocol.registerQueryHandler(request -> runOnContextIfReady(() -> role.onQuery(request), QueryResponse::builder));
  }

  private <R extends RaftResponse> CompletableFuture<R> runOnContextIfReady(
      Supplier<CompletableFuture<R>> function, Supplier<RaftResponse.Builder<?, R>> builderSupplier) {
    if (state == State.READY) {
      return runOnContext(function);
    } else {
      return CompletableFuture.completedFuture(builderSupplier.get()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
          .build());
    }
  }

  private <R extends RaftResponse> CompletableFuture<R> runOnContext(Supplier<CompletableFuture<R>> function) {
    CompletableFuture<R> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      function.get().whenComplete((response, error) -> {
        if (error == null) {
          future.complete(response);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  /**
   * Unregisters server handlers on the configured protocol.
   */
  private void unregisterHandlers(RaftServerProtocol protocol) {
    protocol.unregisterOpenSessionHandler();
    protocol.unregisterCloseSessionHandler();
    protocol.unregisterKeepAliveHandler();
    protocol.unregisterMetadataHandler();
    protocol.unregisterConfigureHandler();
    protocol.unregisterInstallHandler();
    protocol.unregisterJoinHandler();
    protocol.unregisterReconfigureHandler();
    protocol.unregisterLeaveHandler();
    protocol.unregisterTransferHandler();
    protocol.unregisterAppendHandler();
    protocol.unregisterPollHandler();
    protocol.unregisterVoteHandler();
    protocol.unregisterCommandHandler();
    protocol.unregisterQueryHandler();
  }

  /**
   * Attempts to become the leader.
   */
  public CompletableFuture<Void> anoint() {
    if (role.role() == RaftServer.Role.LEADER) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();

    threadContext.execute(() -> {
      // Register a leader election listener to wait for the election of this node.
      Consumer<RaftMember> electionListener = new Consumer<RaftMember>() {
        @Override
        public void accept(RaftMember member) {
          if (member.memberId().equals(cluster.getMember().memberId())) {
            future.complete(null);
          } else {
            future.completeExceptionally(new RaftException.ProtocolException("Failed to transfer leadership"));
          }
          removeLeaderElectionListener(this);
        }
      };
      addLeaderElectionListener(electionListener);

      // If a leader already exists, request a leadership transfer from it. Otherwise, transition to the candidate
      // state and attempt to get elected.
      RaftMember member = getCluster().getMember();
      RaftMember leader = getLeader();
      if (leader != null) {
        protocol.transfer(leader.memberId(), TransferRequest.builder()
            .withMember(member.memberId())
            .build()).whenCompleteAsync((response, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else if (response.status() == RaftResponse.Status.ERROR) {
                future.completeExceptionally(response.error().createException());
              } else {
                transition(RaftServer.Role.CANDIDATE);
              }
            }, threadContext);
      } else {
        transition(RaftServer.Role.CANDIDATE);
      }
    });
    return future;
  }

  /**
   * Transitions the server to the base state for the given member type.
   */
  public void transition(RaftMember.Type type) {
    switch (type) {
      case ACTIVE:
        if (!(role instanceof ActiveRole)) {
          transition(RaftServer.Role.FOLLOWER);
        }
        break;
      case PROMOTABLE:
        if (this.role.role() != RaftServer.Role.PROMOTABLE) {
          transition(RaftServer.Role.PROMOTABLE);
        }
        break;
      case PASSIVE:
        if (this.role.role() != RaftServer.Role.PASSIVE) {
          transition(RaftServer.Role.PASSIVE);
        }
        break;
      default:
        if (this.role.role() != RaftServer.Role.INACTIVE) {
          transition(RaftServer.Role.INACTIVE);
        }
        break;
    }
  }

  /**
   * Transition handler.
   */
  public void transition(RaftServer.Role role) {
    checkThread();
    checkNotNull(role);

    if (this.role.role() == role) {
      return;
    }

    log.info("Transitioning to {}", role);

    // Close the old state.
    try {
      this.role.stop().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to close Raft state", e);
    }

    // Force state transitions to occur synchronously in order to prevent race conditions.
    try {
      this.role = createRole(role);
      this.role.start().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize Raft state", e);
    }

    roleChangeListeners.forEach(l -> l.accept(this.role.role()));
  }

  /**
   * Creates an internal state for the given state type.
   */
  private RaftRole createRole(RaftServer.Role role) {
    switch (role) {
      case INACTIVE:
        return new InactiveRole(this);
      case PASSIVE:
        return new PassiveRole(this);
      case PROMOTABLE:
        return new PromotableRole(this);
      case FOLLOWER:
        return new FollowerRole(this);
      case CANDIDATE:
        return new CandidateRole(this);
      case LEADER:
        return new LeaderRole(this);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void close() {
    // Unregister protocol listeners.
    unregisterHandlers(protocol);

    // Close the log.
    try {
      raftLog.close();
    } catch (Exception e) {
    }

    // Close the metastore.
    try {
      meta.close();
    } catch (Exception e) {
    }

    // Close the snapshot store.
    try {
      snapshotStore.close();
    } catch (Exception e) {
    }

    // Close the state machine and thread context.
    stateMachine.close();
    threadContext.close();
    loadContext.close();
    stateContext.close();

    // Only close the thread context factory if indicated.
    if (closeOnStop) {
      threadContextFactory.close();
    }
  }

  /**
   * Deletes the server context.
   */
  public void delete() {
    // Delete the log.
    storage.deleteLog();

    // Delete the snapshot store.
    storage.deleteSnapshotStore();

    // Delete the metadata store.
    storage.deleteMetaStore();

    // Unlock the store.
    storage.unlock();
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

  /**
   * Raft server state.
   */
  public enum State {
    ACTIVE,
    READY,
  }

}
