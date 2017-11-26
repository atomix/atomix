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

import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.Sessions;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
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
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupSerializers;
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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server state machine executor.
 */
public class PrimaryBackupServiceContext implements ServiceContext {
  private static final Serializer SERIALIZER = PrimaryBackupSerializers.PROTOCOL;
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(1);

  private final Logger log;
  private final NodeId localNodeId;
  private final String serverName;
  private final PrimitiveId primitiveId;
  private final PrimitiveType primitiveType;
  private final PrimitiveDescriptor descriptor;
  private final PrimitiveService service;
  private final PrimaryBackupServiceSessions sessions = new PrimaryBackupServiceSessions();
  private final ThreadContext threadContext;
  private final ClusterService clusterService;
  private final PrimaryBackupServerProtocol protocol;
  private final PrimaryElection primaryElection;
  private NodeId primary;
  private List<NodeId> backups;
  private long currentTerm;
  private long currentIndex;
  private long commitIndex;
  private WallClockTimestamp currentTimestamp = new WallClockTimestamp();
  private OperationType currentOperation = OperationType.COMMAND;
  private final LogicalClock logicalClock = new LogicalClock() {
    @Override
    public LogicalTimestamp time() {
      return new LogicalTimestamp(currentIndex);
    }
  };
  private final WallClock wallClock = new WallClock() {
    @Override
    public WallClockTimestamp time() {
      return currentTimestamp;
    }
  };
  private PrimaryBackupRole role;
  private final ClusterEventListener clusterEventListener = this::handleClusterEvent;
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeRole(event.term());

  public PrimaryBackupServiceContext(
      String serverName,
      PrimitiveId primitiveId,
      PrimitiveType primitiveType,
      PrimitiveDescriptor descriptor,
      ThreadContext threadContext,
      ClusterService clusterService,
      PrimaryBackupServerProtocol protocol,
      PrimaryElection primaryElection) {
    this.localNodeId = clusterService.getLocalNode().id();
    this.serverName = checkNotNull(serverName);
    this.primitiveId = checkNotNull(primitiveId);
    this.primitiveType = checkNotNull(primitiveType);
    this.descriptor = checkNotNull(descriptor);
    this.service = primitiveType.newService();
    this.threadContext = checkNotNull(threadContext);
    this.clusterService = checkNotNull(clusterService);
    this.protocol = checkNotNull(protocol);
    this.primaryElection = checkNotNull(primaryElection);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(serverName)
        .add("type", descriptor.type())
        .add("name", descriptor.name())
        .build());
    init();
    clusterService.addListener(clusterEventListener);
    primaryElection.addListener(primaryElectionListener);
    changeRole(primaryElection.getTerm());
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    sessions.addListener(service);
    service.init(this);
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterEvent event) {
    if (event.type() == ClusterEvent.Type.NODE_DEACTIVATED) {
      for (Session session : sessions) {
        if (session.nodeId().equals(event.subject().id())) {
          sessions.expireSession((PrimaryBackupSession) session);
        }
      }
    }
  }

  /**
   * Changes the roles.
   */
  private void changeRole(PrimaryTerm term) {
    if (term.term() > currentTerm) {
      currentTerm = term.term();
      primary = term.primary();
      backups = term.backups().subList(0, Math.min(descriptor.backups(), term.backups().size()));

      if (term.primary().equals(clusterService.getLocalNode().id())) {
        if (this.role == null) {
          this.role = new PrimaryRole(this);
          log.trace("{} transitioning to {}", clusterService.getLocalNode().id(), Role.PRIMARY);
        } else if (this.role.role() != Role.PRIMARY) {
          this.role.close();
          this.role = new PrimaryRole(this);
          log.trace("{} transitioning to {}", clusterService.getLocalNode().id(), Role.PRIMARY);
        }
      } else if (term.backups().contains(clusterService.getLocalNode().id())) {
        if (this.role == null) {
          this.role = new BackupRole(this);
          log.trace("{} transitioning to {}", clusterService.getLocalNode().id(), Role.BACKUP);
        } else if (this.role.role() != Role.BACKUP) {
          this.role.close();
          this.role = new BackupRole(this);
          log.trace("{} transitioning to {}", clusterService.getLocalNode().id(), Role.BACKUP);
        }
      } else {
        if (this.role == null) {
          this.role = new NoneRole(this);
          log.trace("{} transitioning to {}", clusterService.getLocalNode().id(), Role.NONE);
        } else if (this.role.role() != Role.NONE) {
          this.role.close();
          this.role = new NoneRole(this);
          log.trace("{} transitioning to {}", clusterService.getLocalNode().id(), Role.NONE);
        }
      }
    }
  }

  @Override
  public PrimitiveId serviceId() {
    return primitiveId;
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
   * Returns the local node ID.
   *
   * @return the local node ID
   */
  public NodeId nodeId() {
    return localNodeId;
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
  public long currentIndex() {
    return currentIndex;
  }

  /**
   * Returns the current wall clock timestamp.
   *
   * @return the current wall clock timestamp
   */
  public WallClockTimestamp currentTimestamp() {
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
   * @param term the term to which to reset the current term
   * @param primary the primary for the given term
   */
  public void resetTerm(long term, NodeId primary) {
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
    return ++currentIndex;
  }

  /**
   * Increments the current index and returns true if the given index is the next index.
   *
   * @param index the index to which to increment the current index
   * @param timestamp the timestamp at the next index
   * @return indicates whether the current index was successfully incremented
   */
  public boolean nextIndex(long index, long timestamp) {
    if (currentIndex + 1 == index) {
      currentOperation = OperationType.COMMAND;
      currentIndex++;
      currentTimestamp = new WallClockTimestamp(timestamp);
      return true;
    }
    return false;
  }

  /**
   * Resets the current index to the given index and timestamp.
   *
   * @param index the index to which to reset the current index
   * @param timestamp the timestamp to which to reset the current timestamp
   */
  public void resetIndex(long index, long timestamp) {
    currentOperation = OperationType.COMMAND;
    currentIndex = index;
    currentTimestamp = WallClockTimestamp.from(timestamp);
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
   * Increments and returns the wall clock timestamp.
   *
   * @return the wall clock timestamp
   */
  public WallClockTimestamp nextTimestamp() {
    currentTimestamp = new WallClockTimestamp();
    service.tick(currentTimestamp);
    return currentTimestamp;
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

  @Override
  public Sessions sessions() {
    return sessions;
  }

  /**
   * Returns the primary node.
   *
   * @return the primary node
   */
  public NodeId primary() {
    return primary;
  }

  /**
   * Returns the backup nodes.
   *
   * @return the backup nodes
   */
  public List<NodeId> backups() {
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
   * Gets or creates a service session.
   *
   * @param sessionId the session to get
   * @return the service session
   */
  public Session getSession(long sessionId) {
    return sessions.getSession(sessionId);
  }

  /**
   * Gets or creates a service session.
   *
   * @param sessionId the session to create
   * @param nodeId the owning node ID
   * @return the service session
   */
  public PrimaryBackupSession getOrCreateSession(long sessionId, NodeId nodeId) {
    PrimaryBackupSession session = sessions.getSession(sessionId);
    if (session == null) {
      session = new PrimaryBackupSession(
          SessionId.from(sessionId),
          descriptor.name(),
          primitiveType,
          nodeId,
          protocol);
      sessions.openSession(session);
    }
    return session;
  }

  /**
   * Closes the service.
   */
  void close() {
    clusterService.removeListener(clusterEventListener);
    primaryElection.removeListener(primaryElectionListener);
  }
}
