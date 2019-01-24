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

package io.atomix.protocols.backup.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroupService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.CloseResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupServerProtocol;
import io.atomix.protocols.backup.protocol.PrimitiveDescriptor;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.protocols.backup.roles.BackupRole;
import io.atomix.protocols.backup.roles.NoneRole;
import io.atomix.protocols.backup.roles.PrimaryBackupRole;
import io.atomix.protocols.backup.roles.PrimaryRole;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server state machine executor.
 */
public class PrimaryBackupServiceContext implements ServiceContext {
  private final Logger log;
  private final MemberId localMemberId;
  private final String serverName;
  private final PrimitiveId primitiveId;
  private final PrimitiveType primitiveType;
  private final ServiceConfig serviceConfig;
  private final PrimitiveDescriptor descriptor;
  private final PrimitiveService service;
  private final Map<Long, PrimaryBackupSession> sessions = Maps.newConcurrentMap();
  private final ThreadContext threadContext;
  private final ClusterMembershipService clusterMembershipService;
  private final MemberGroupService memberGroupService;
  private final PrimaryBackupServerProtocol protocol;
  private final PrimaryElection primaryElection;
  private MemberId primary;
  private List<MemberId> backups;
  private long currentTerm;
  private long currentIndex;
  private Session currentSession;
  private long currentTimestamp;
  private long operationIndex;
  private long commitIndex;
  private OperationType currentOperation = OperationType.COMMAND;
  private final LogicalClock logicalClock = new LogicalClock() {
    @Override
    public LogicalTimestamp getTime() {
      return new LogicalTimestamp(operationIndex);
    }
  };
  private final WallClock wallClock = new WallClock() {
    @Override
    public WallClockTimestamp getTime() {
      return WallClockTimestamp.from(currentTimestamp);
    }
  };
  private PrimaryBackupRole role;
  private final ClusterMembershipEventListener membershipEventListener = this::handleClusterEvent;
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeRole(event.term());

  @SuppressWarnings("unchecked")
  public PrimaryBackupServiceContext(
      String serverName,
      PrimitiveId primitiveId,
      PrimitiveType primitiveType,
      PrimitiveDescriptor descriptor,
      ThreadContext threadContext,
      ClusterMembershipService clusterMembershipService,
      MemberGroupService memberGroupService,
      PrimaryBackupServerProtocol protocol,
      PrimaryElection primaryElection) {
    this.localMemberId = clusterMembershipService.getLocalMember().id();
    this.serverName = checkNotNull(serverName);
    this.primitiveId = checkNotNull(primitiveId);
    this.primitiveType = checkNotNull(primitiveType);
    this.serviceConfig = Serializer.using(primitiveType.namespace()).decode(descriptor.config());
    this.descriptor = checkNotNull(descriptor);
    this.service = primitiveType.newService(serviceConfig);
    this.threadContext = checkNotNull(threadContext);
    this.clusterMembershipService = checkNotNull(clusterMembershipService);
    this.memberGroupService = checkNotNull(memberGroupService);
    this.protocol = checkNotNull(protocol);
    this.primaryElection = checkNotNull(primaryElection);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(serverName)
        .add("type", descriptor.type())
        .add("name", descriptor.name())
        .build());
    clusterMembershipService.addListener(membershipEventListener);
    primaryElection.addListener(primaryElectionListener);
  }

  /**
   * Opens the service context.
   *
   * @return a future to be completed once the service context has been opened
   */
  public CompletableFuture<Void> open() {
    return primaryElection.getTerm()
        .thenAccept(this::changeRole)
        .thenRun(() -> service.init(this));
  }

  /**
   * Returns the current service role.
   *
   * @return the current service role
   */
  public Role getRole() {
    return role.role();
  }

  @Override
  public PrimitiveId serviceId() {
    return primitiveId;
  }

  @Override
  public MemberId localMemberId() {
    return memberId();
  }

  /**
   * Returns the primitive descriptor.
   *
   * @return the primitive descriptor
   */
  public PrimitiveDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns the local member ID.
   *
   * @return the local member ID
   */
  public MemberId memberId() {
    return localMemberId;
  }

  /**
   * Returns the server name.
   *
   * @return the server name
   */
  public String serverName() {
    return serverName;
  }

  @Override
  public String serviceName() {
    return descriptor.name();
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends ServiceConfig> C serviceConfig() {
    return (C) serviceConfig;
  }

  @Override
  public long currentIndex() {
    return currentIndex;
  }

  @Override
  public Session currentSession() {
    return currentSession;
  }

  /**
   * Returns the current wall clock timestamp.
   *
   * @return the current wall clock timestamp
   */
  public long currentTimestamp() {
    return currentTimestamp;
  }

  /**
   * Sets the current timestamp.
   *
   * @param timestamp the updated timestamp
   * @return the current timestamp
   */
  public long setTimestamp(long timestamp) {
    this.currentTimestamp = timestamp;
    service.tick(WallClockTimestamp.from(timestamp));
    return currentTimestamp;
  }

  /**
   * Returns the current term.
   *
   * @return the current term
   */
  public long currentTerm() {
    return currentTerm;
  }

  /**
   * Resets the current term to the given term.
   *
   * @param term    the term to which to reset the current term
   * @param primary the primary for the given term
   */
  public void resetTerm(long term, MemberId primary) {
    this.currentTerm = term;
    this.primary = primary;
  }

  /**
   * Increments and returns the next service index.
   *
   * @return the next index
   */
  public long nextIndex() {
    currentOperation = OperationType.COMMAND;
    return ++operationIndex;
  }

  /**
   * Increments the current index and returns true if the given index is the next index.
   *
   * @param index the index to which to increment the current index
   * @return indicates whether the current index was successfully incremented
   */
  public boolean nextIndex(long index) {
    if (operationIndex + 1 == index) {
      currentOperation = OperationType.COMMAND;
      operationIndex++;
      return true;
    }
    return false;
  }

  /**
   * Resets the current index to the given index and timestamp.
   *
   * @param index     the index to which to reset the current index
   * @param timestamp the timestamp to which to reset the current timestamp
   */
  public void resetIndex(long index, long timestamp) {
    currentOperation = OperationType.COMMAND;
    operationIndex = index;
    currentIndex = index;
    currentTimestamp = timestamp;
    setCommitIndex(index);
    service.tick(new WallClockTimestamp(currentTimestamp));
  }

  /**
   * Sets the current index.
   *
   * @param index the current index.
   * @return the current index
   */
  public long setIndex(long index) {
    currentOperation = OperationType.COMMAND;
    currentIndex = index;
    return currentIndex;
  }

  /**
   * Returns the current service index and sets the service to read-only mode.
   *
   * @return the current index
   */
  public long getIndex() {
    currentOperation = OperationType.QUERY;
    return currentIndex;
  }

  /**
   * Sets the current session.
   *
   * @param session the current session
   * @return the updated session
   */
  public Session setSession(Session session) {
    this.currentSession = session;
    return session;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex the commit index
   * @return the updated commit index
   */
  public long setCommitIndex(long commitIndex) {
    this.commitIndex = Math.max(this.commitIndex, commitIndex);
    return this.commitIndex;
  }

  /**
   * Returns the current commit index.
   *
   * @return the current commit index
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  @Override
  public OperationType currentOperation() {
    return currentOperation;
  }

  @Override
  public LogicalClock logicalClock() {
    return logicalClock;
  }

  @Override
  public WallClock wallClock() {
    return wallClock;
  }

  /**
   * Returns the primary node.
   *
   * @return the primary node
   */
  public MemberId primary() {
    return primary;
  }

  /**
   * Returns the backup nodes.
   *
   * @return the backup nodes
   */
  public List<MemberId> backups() {
    return backups;
  }

  /**
   * Returns the service thread context.
   *
   * @return the service thread context
   */
  public ThreadContext threadContext() {
    return threadContext;
  }

  /**
   * Returns the server protocol.
   *
   * @return the server protocol
   */
  public PrimaryBackupServerProtocol protocol() {
    return protocol;
  }

  /**
   * Returns the primitive service instance.
   *
   * @return the primitive service instance
   */
  public PrimitiveService service() {
    return service;
  }

  /**
   * Handles an execute request.
   *
   * @param request the execute request
   * @return future to be completed with an execute response
   */
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    ComposableFuture<ExecuteResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      role.execute(request).whenComplete(future);
    });
    return future;
  }

  /**
   * Handles a backup request.
   *
   * @param request the backup request
   * @return future to be completed with a backup response
   */
  public CompletableFuture<BackupResponse> backup(BackupRequest request) {
    ComposableFuture<BackupResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      role.backup(request).whenComplete(future);
    });
    return future;
  }

  /**
   * Handles a restore request.
   *
   * @param request the restore request
   * @return future to be completed with a restore response
   */
  public CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    ComposableFuture<RestoreResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      role.restore(request).whenComplete(future);
    });
    return future;
  }

  /**
   * Handles a close request.
   *
   * @param request the close request
   * @return future to be completed with a close response
   */
  public CompletableFuture<CloseResponse> close(CloseRequest request) {
    ComposableFuture<CloseResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      PrimaryBackupSession session = sessions.get(request.session());
      if (session != null) {
        role.close(session).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(CloseResponse.ok());
          } else {
            future.complete(CloseResponse.error());
          }
        });
      } else {
        future.complete(CloseResponse.error());
      }
    });
    return future;
  }

  /**
   * Returns the collection of sessions.
   *
   * @return the collection of sessions
   */
  public Collection<PrimaryBackupSession> getSessions() {
    return ImmutableList.copyOf(sessions.values());
  }

  /**
   * Gets a service session.
   *
   * @param sessionId the session to get
   * @return the service session
   */
  public PrimaryBackupSession getSession(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Creates a service session.
   *
   * @param sessionId the session to create
   * @param memberId  the owning node ID
   * @return the service session
   */
  public PrimaryBackupSession createSession(long sessionId, MemberId memberId) {
    PrimaryBackupSession session = new PrimaryBackupSession(SessionId.from(sessionId), memberId, service.serializer(), this);
    if (sessions.putIfAbsent(sessionId, session) == null) {
      service.register(session);
    }
    return session;
  }

  /**
   * Gets or creates a service session.
   *
   * @param sessionId the session to create
   * @param memberId  the owning node ID
   * @return the service session
   */
  public PrimaryBackupSession getOrCreateSession(long sessionId, MemberId memberId) {
    PrimaryBackupSession session = sessions.get(sessionId);
    if (session == null) {
      session = createSession(sessionId, memberId);
    }
    return session;
  }

  /**
   * Expires the session with the given ID.
   *
   * @param sessionId the session ID
   */
  public void expireSession(long sessionId) {
    PrimaryBackupSession session = sessions.remove(sessionId);
    if (session != null) {
      log.debug("Expiring session {}", session.sessionId());
      session.expire();
      service.expire(session.sessionId());
    }
  }

  /**
   * Closes the session with the given ID.
   *
   * @param sessionId the session ID
   */
  public void closeSession(long sessionId) {
    PrimaryBackupSession session = sessions.remove(sessionId);
    if (session != null) {
      log.debug("Closing session {}", session.sessionId());
      session.close();
      service.close(session.sessionId());
    }
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterMembershipEvent event) {
    threadContext.execute(() -> {
      if (event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED) {
        for (Session session : sessions.values()) {
          if (session.memberId().equals(event.subject().id())) {
            role.expire((PrimaryBackupSession) session);
          }
        }
      }
    });
  }

  /**
   * Changes the roles.
   */
  private void changeRole(PrimaryTerm term) {
    threadContext.execute(() -> {
      if (term.term() > currentTerm) {
        log.debug("Term changed: {}", term);
        currentTerm = term.term();
        primary = term.primary() != null ? term.primary().memberId() : null;
        backups = term.backups(descriptor.backups())
            .stream()
            .map(GroupMember::memberId)
            .collect(Collectors.toList());

        if (Objects.equals(primary, clusterMembershipService.getLocalMember().id())) {
          if (this.role == null) {
            this.role = new PrimaryRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.PRIMARY);
          } else if (this.role.role() != Role.PRIMARY) {
            this.role.close();
            this.role = new PrimaryRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.PRIMARY);
          }
        } else if (backups.contains(clusterMembershipService.getLocalMember().id())) {
          if (this.role == null) {
            this.role = new BackupRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.BACKUP);
          } else if (this.role.role() != Role.BACKUP) {
            this.role.close();
            this.role = new BackupRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.BACKUP);
          }
        } else {
          if (this.role == null) {
            this.role = new NoneRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.NONE);
          } else if (this.role.role() != Role.NONE) {
            this.role.close();
            this.role = new NoneRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.NONE);
          }
        }
      }
    });
  }

  /**
   * Closes the service.
   */
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      try {
        clusterMembershipService.removeListener(membershipEventListener);
        primaryElection.removeListener(primaryElectionListener);
        role.close();
      } finally {
        future.complete(null);
      }
    });
    return future.thenRunAsync(() -> threadContext.close());
  }
}
