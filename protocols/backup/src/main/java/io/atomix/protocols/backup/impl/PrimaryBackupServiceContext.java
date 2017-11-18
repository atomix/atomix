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

package io.atomix.protocols.backup.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.Sessions;
import io.atomix.protocols.backup.ReplicaInfo;
import io.atomix.protocols.backup.ReplicaInfo.Role;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.CloseSessionOperation;
import io.atomix.protocols.backup.protocol.CloseSessionRequest;
import io.atomix.protocols.backup.protocol.CloseSessionResponse;
import io.atomix.protocols.backup.protocol.ExecuteOperation;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.ExpireSessionOperation;
import io.atomix.protocols.backup.protocol.HeartbeatOperation;
import io.atomix.protocols.backup.protocol.OpenSessionOperation;
import io.atomix.protocols.backup.protocol.OpenSessionRequest;
import io.atomix.protocols.backup.protocol.OpenSessionResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server state machine executor.
 */
public class PrimaryBackupServiceContext implements ServiceContext {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC); // TODO
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(1);

  private final Logger log;
  private final NodeId localNodeId;
  private final String serverName;
  private final PrimitiveId primitiveId;
  private final String serviceName;
  private final PrimitiveType primitiveType;
  private final PrimitiveService service;
  private final DefaultServiceSessions sessions = new DefaultServiceSessions();
  private final ThreadContext threadContext;
  private final ClusterService clusterService;
  private final ClusterCommunicationService clusterCommunicator;
  private final ReplicaInfoProvider replicaProvider;
  private NodeId primary;
  private List<NodeId> backups;
  private long currentTerm;
  private long currentIndex;
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
  private final MessageSubject executeSubject;
  private final MessageSubject backupSubject;
  private final MessageSubject restoreSubject;
  private final ClusterEventListener clusterEventListener = this::handleClusterEvent;
  private final Consumer<ReplicaInfo> replicaInfoListener = this::changeRole;

  public PrimaryBackupServiceContext(
      String serverName,
      PrimitiveId primitiveId,
      String serviceName,
      PrimitiveType primitiveType,
      ThreadContext threadContext,
      ClusterService clusterService,
      ClusterCommunicationService clusterCommunicator,
      ReplicaInfoProvider replicaProvider) {
    this.localNodeId = clusterService.getLocalNode().id();
    this.serverName = checkNotNull(serverName);
    this.primitiveId = checkNotNull(primitiveId);
    this.serviceName = checkNotNull(serviceName);
    this.primitiveType = checkNotNull(primitiveType);
    this.service = primitiveType.newService();
    this.threadContext = checkNotNull(threadContext);
    this.clusterService = checkNotNull(clusterService);
    this.clusterCommunicator = checkNotNull(clusterCommunicator);
    this.replicaProvider = checkNotNull(replicaProvider);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(primitiveId)
        .add("type", primitiveType)
        .add("name", serviceName)
        .build());
    this.executeSubject = new MessageSubject(String.format("%s-%s-execute", serverName, serviceName));
    this.backupSubject = new MessageSubject(String.format("%s-%s-backup", serverName, serviceName));
    this.restoreSubject = new MessageSubject(String.format("%s-%s-restore", serverName, serviceName));
    init();
    registerSubscribers();
    clusterService.addListener(clusterEventListener);
    replicaProvider.addChangeListener(replicaInfoListener);
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
      role.expire(event.subject().id());
    }
  }

  /**
   * Changes the roles.
   */
  private void changeRole(ReplicaInfo replicaInfo) {
    if (replicaInfo.term() > currentTerm) {
      currentTerm = replicaInfo.term();
      primary = replicaInfo.primary();
      backups = replicaInfo.backups();

      Role role = replicaInfo.roleFor(clusterService.getLocalNode().id());
      switch (role) {
        case PRIMARY:
          if (this.role.role() != Role.PRIMARY) {
            this.role.close();
            this.role = new PrimaryRole();
          }
          break;
        case BACKUP:
          if (this.role.role() != Role.BACKUP) {
            this.role.close();
            this.role = new BackupRole();
          }
          break;
        case NONE:
          if (this.role.role() != Role.NONE) {
            this.role.close();
            this.role = new NoneRole();
          }
          break;
      }
    }
  }

  /**
   * Registers subscribers.
   */
  private void registerSubscribers() {
    clusterCommunicator.<ExecuteRequest, ExecuteResponse>addSubscriber(
        executeSubject,
        SERIALIZER::decode,
        r -> role.execute(r),
        SERIALIZER::encode,
        threadContext);
    clusterCommunicator.<BackupRequest>addSubscriber(
        backupSubject,
        SERIALIZER::decode,
        r -> role.backup(r),
        threadContext);
    clusterCommunicator.<RestoreRequest, RestoreResponse>addSubscriber(
        restoreSubject,
        SERIALIZER::decode,
        r -> role.restore(r),
        SERIALIZER::encode,
        threadContext);
  }

  /**
   * Unregisters subscribers.
   */
  private void unregisterSubscribers() {
    clusterCommunicator.removeSubscriber(executeSubject);
    clusterCommunicator.removeSubscriber(backupSubject);
    clusterCommunicator.removeSubscriber(restoreSubject);
  }

  @Override
  public PrimitiveId serviceId() {
    return primitiveId;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  public long currentIndex() {
    return currentIndex;
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
   * Handles an open session request.
   *
   * @param request the open session request
   * @return future to be completed with the open session response
   */
  CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    ComposableFuture<OpenSessionResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      future.complete(role.openSession(request));
    });
    return future;
  }

  /**
   * Handles a close session request.
   *
   * @param request the close session request
   * @return future to be completed with the close session response
   */
  CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    ComposableFuture<CloseSessionResponse> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      future.complete(role.closeSession(request));
    });
    return future;
  }

  /**
   * Closes the service.
   */
  void close() {
    unregisterSubscribers();
    clusterService.removeListener(clusterEventListener);
    replicaProvider.removeChangeListener(replicaInfoListener);
  }

  /**
   * Base class for internal roles.
   */
  private abstract class PrimaryBackupRole {
    private final Role role;

    PrimaryBackupRole(Role role) {
      this.role = role;
    }

    Role role() {
      return role;
    }

    /**
     * Handles an open session request.
     *
     * @param request the open session request
     * @return the open session response
     */
    OpenSessionResponse openSession(OpenSessionRequest request) {
      return new OpenSessionResponse(Status.ERROR, 0);
    }

    /**
     * Handles a close session request.
     *
     * @param request the close session request
     * @return the close session response
     */
    CloseSessionResponse closeSession(CloseSessionRequest request) {
      return new CloseSessionResponse(Status.ERROR);
    }

    /**
     * Handles an execute request.
     *
     * @param request the execute request
     * @return the execute response
     */
    ExecuteResponse execute(ExecuteRequest request) {
      return new ExecuteResponse(Status.ERROR, null);
    }

    /**
     * Handles a backup request.
     *
     * @param request the backup request
     */
    void backup(BackupRequest request) {
    }

    /**
     * Handles a restore request.
     *
     * @param request the restore request
     * @return the restore response
     */
    RestoreResponse restore(RestoreRequest request) {
      return new RestoreResponse(Status.ERROR, 0, 0, null);
    }

    /**
     * Handles an expired node.
     *
     * @param nodeId the expired node identifier
     */
    void expire(NodeId nodeId) {
    }

    /**
     * Closes the role.
     */
    void close() {
    }
  }

  /**
   * Primary role.
   */
  private class PrimaryRole extends PrimaryBackupRole {
    private static final int MAX_BATCH_SIZE = 100;
    private static final long MAX_BATCH_TIME = 1000;

    private final Map<NodeId, BackupQueue> queues = Maps.newConcurrentMap();
    private Scheduled heartbeatSchedule;

    PrimaryRole() {
      super(Role.PRIMARY);
      heartbeatSchedule = threadContext.schedule(HEARTBEAT_INTERVAL, this::heartbeat);
    }

    private long incrementIndex() {
      currentOperation = OperationType.COMMAND;
      return ++currentIndex;
    }

    private long getIndex() {
      currentOperation = OperationType.QUERY;
      return currentIndex;
    }

    private WallClockTimestamp incrementTimestamp() {
      currentTimestamp = new WallClockTimestamp();
      service.tick(currentTimestamp);
      return currentTimestamp;
    }

    @Override
    OpenSessionResponse openSession(OpenSessionRequest request) {
      long index = incrementIndex();
      WallClockTimestamp timestamp = incrementTimestamp();
      PrimaryBackupSession session = new PrimaryBackupSession(
          serverName,
          SessionId.from(index),
          serviceName,
          primitiveType,
          request.nodeId(),
          clusterService,
          clusterCommunicator);
      sessions.openSession(session);
      backup(new OpenSessionOperation(index, timestamp.unixTimestamp(), request.nodeId()));
      return new OpenSessionResponse(Status.OK, session.sessionId().id());
    }

    @Override
    CloseSessionResponse closeSession(CloseSessionRequest request) {
      long index = incrementIndex();
      WallClockTimestamp timestamp = incrementTimestamp();
      PrimaryBackupSession session = (PrimaryBackupSession) sessions.getSession(request.sessionId());
      if (session == null) {
        return new CloseSessionResponse(Status.ERROR);
      }
      sessions.closeSession(session);
      backup(new CloseSessionOperation(index, timestamp.unixTimestamp(), request.sessionId()));
      return new CloseSessionResponse(Status.OK);
    }

    @Override
    void expire(NodeId nodeId) {
      for (Session session : sessions) {
        if (session.nodeId().equals(nodeId)) {
          long index = incrementIndex();
          WallClockTimestamp timestamp = incrementTimestamp();
          sessions.closeSession((PrimaryBackupSession) session);
          backup(new ExpireSessionOperation(index, timestamp.unixTimestamp(), session.sessionId().id()));
        }
      }
    }

    @Override
    ExecuteResponse execute(ExecuteRequest request) {
      Session session = sessions.getSession(request.sessionId());
      if (session == null) {
        return new ExecuteResponse(Status.ERROR, null);
      }

      if (request.operation().id().type() == OperationType.COMMAND) {
        return executeCommand(request, session);
      } else {
        return executeQuery(request, session);
      }
    }

    private ExecuteResponse executeCommand(ExecuteRequest request, Session session) {
      long index = incrementIndex();
      WallClockTimestamp timestamp = incrementTimestamp();
      try {
        byte[] result = service.apply(new DefaultCommit<>(
            index,
            request.operation().id(),
            request.operation().value(),
            session,
            timestamp.unixTimestamp()));
        return new ExecuteResponse(Status.OK, result);
      } catch (Exception e) {
        return new ExecuteResponse(Status.ERROR, null);
      } finally {
        backup(new ExecuteOperation(
            index,
            timestamp.unixTimestamp(),
            session.sessionId().id(),
            request.operation()));
      }
    }

    private ExecuteResponse executeQuery(ExecuteRequest request, Session session) {
      try {
        byte[] result = service.apply(new DefaultCommit<>(
            getIndex(),
            request.operation().id(),
            request.operation().value(),
            session,
            currentTimestamp.unixTimestamp()));
        return new ExecuteResponse(Status.OK, result);
      } catch (Exception e) {
        return new ExecuteResponse(Status.ERROR, null);
      }
    }

    @Override
    RestoreResponse restore(RestoreRequest request) {
      if (request.term() != currentTerm) {
        return new RestoreResponse(Status.ERROR, 0, 0, null);
      }

      HeapBuffer buffer = HeapBuffer.allocate();
      service.backup(buffer);
      buffer.flip();
      byte[] bytes = buffer.readBytes(buffer.remaining());
      return new RestoreResponse(Status.OK, currentIndex, currentTimestamp.unixTimestamp(), bytes);
    }

    /**
     * Backs up the given operation.
     */
    private void backup(BackupOperation operation) {
      for (NodeId backup : backups) {
        backup(backup, operation);
      }
    }

    /**
     * Backs up the given operation to the given node.
     */
    private void backup(NodeId nodeId, BackupOperation operation) {
      queues.computeIfAbsent(nodeId, n -> new BackupQueue(n, MAX_BATCH_SIZE, MAX_BATCH_TIME)).add(operation);
    }

    /**
     * Creates and replicates a heartbeat.
     */
    private void heartbeat() {
      WallClockTimestamp timestamp = new WallClockTimestamp();
      service.tick(timestamp);
      sendHeartbeats(new HeartbeatOperation(currentTerm, timestamp.unixTimestamp()));
    }

    /**
     * Sends heartbeats to all backups.
     */
    private void sendHeartbeats(HeartbeatOperation operation) {
      for (NodeId backup : backups) {
        sendHeartbeat(backup, operation);
      }
    }

    /**
     * Sends a heartbeat to the given node if necessary.
     */
    private void sendHeartbeat(NodeId nodeId, HeartbeatOperation operation) {
      BackupQueue queue = queues.computeIfAbsent(nodeId, n -> new BackupQueue(n, MAX_BATCH_SIZE, MAX_BATCH_TIME));
      queue.add(operation);
      queue.maybeSendBatch();
    }

    @Override
    void close() {
      heartbeatSchedule.cancel();
    }
  }

  /**
   * Backup role.
   */
  private class BackupRole extends PrimaryBackupRole {
    BackupRole() {
      super(Role.BACKUP);
    }

    @Override
    void backup(BackupRequest request) {
      // If the term is greater than the node's current term, update the term.
      if (request.term() > currentTerm) {
        primary = request.primary();
        currentTerm = request.term();
      }
      // If the term is less than the node's current term, ignore the backup message.
      else if (request.term() < currentTerm) {
        return;
      }

      for (BackupOperation operation : request.operations()) {
        // If the operation's index is greater than the next index, request a snapshot from the primary.
        if (operation.index() == currentIndex + 1) {
          switch (operation.type()) {
            case OPEN_SESSION:
              applyOpenSession((OpenSessionOperation) operation);
              break;
            case CLOSE_SESSION:
              applyCloseSession((CloseSessionOperation) operation);
              break;
            case EXECUTE:
              applyExecute((ExecuteOperation) operation);
              break;
            case HEARTBEAT:
              break;
          }
        } else {
          requestRestore(request.primary());
          break;
        }
      }
    }

    private long setIndex(long index) {
      currentIndex = index;
      return currentIndex;
    }

    private WallClockTimestamp setTimestamp(long timestamp) {
      if (currentTimestamp.unixTimestamp() < timestamp) {
        currentTimestamp = new WallClockTimestamp(timestamp);
        service.tick(currentTimestamp);
      }
      return currentTimestamp;
    }

    private void applyOpenSession(OpenSessionOperation operation) {
      long index = setIndex(operation.index());
      setTimestamp(operation.timestamp());
      PrimaryBackupSession session = new PrimaryBackupSession(
          serverName,
          SessionId.from(index),
          serviceName,
          primitiveType,
          operation.nodeId(),
          clusterService,
          clusterCommunicator);
      sessions.openSession(session);
    }

    private void applyCloseSession(CloseSessionOperation operation) {
      setIndex(operation.index());
      setTimestamp(operation.timestamp());
      PrimaryBackupSession session = (PrimaryBackupSession) sessions.getSession(operation.sessionId());
      if (session != null) {
        sessions.closeSession(session);
      }
    }

    private void applyExecute(ExecuteOperation operation) {
      Session session = sessions.getSession(operation.session());
      if (session != null) {
        long index = setIndex(operation.index());
        WallClockTimestamp timestamp = setTimestamp(operation.timestamp());
        service.apply(new DefaultCommit<>(
            index,
            operation.operation().id(),
            operation.operation().value(),
            session,
            timestamp.unixTimestamp()));
      }
    }

    /**
     * Requests a restore from the primary.
     */
    private void requestRestore(NodeId primary) {
      clusterCommunicator.<RestoreRequest, RestoreResponse>sendAndReceive(
          restoreSubject,
          new RestoreRequest(currentTerm),
          SERIALIZER::encode,
          SERIALIZER::decode,
          primary)
          .whenCompleteAsync((response, error) -> {
            if (error == null) {
              currentIndex = response.index();
              currentTimestamp = new WallClockTimestamp(response.timestamp());
              service.restore(HeapBuffer.wrap(response.data()));
            }
          }, threadContext);
    }
  }

  /**
   * None role.
   */
  private class NoneRole extends PrimaryBackupRole {
    NoneRole() {
      super(Role.NONE);
    }
  }

  /**
   * Backup queue.
   */
  private final class BackupQueue {
    private final Queue<BackupOperation> operations = new LinkedList<>();
    private final NodeId nodeId;
    private final int maxBatchSize;
    private final long maxBatchTime;
    private long lastSent;

    BackupQueue(NodeId nodeId, int maxBatchSize, long maxBatchTime) {
      this.nodeId = nodeId;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
    }

    void add(BackupOperation operation) {
      operations.add(operation);
      if (operations.size() >= maxBatchSize) {
        sendBatch();
      }
    }

    void maybeSendBatch() {
      if (System.currentTimeMillis() - lastSent > maxBatchTime && !operations.isEmpty()) {
        sendBatch();
      }
    }

    void sendBatch() {
      List<BackupOperation> batch = ImmutableList.copyOf(operations);
      operations.clear();
      BackupRequest request = new BackupRequest(localNodeId, currentTerm, batch);
      clusterCommunicator.unicast(backupSubject, request, SERIALIZER::encode, nodeId);
      lastSent = batch.size();
    }
  }
}
