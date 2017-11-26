/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.roles;

import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.CloseOperation;
import io.atomix.protocols.backup.protocol.ExecuteOperation;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.ExpireOperation;
import io.atomix.protocols.backup.protocol.HeartbeatOperation;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.time.WallClockTimestamp;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Primary role.
 */
public class PrimaryRole extends PrimaryBackupRole {
  private static final long HEARTBEAT_FREQUENCY = 1000;

  private final Replicator replicator;
  private Scheduled heartbeatTimer;

  public PrimaryRole(PrimaryBackupServiceContext context) {
    super(Role.PRIMARY, context);
    heartbeatTimer = context.threadContext().schedule(
        Duration.ofMillis(HEARTBEAT_FREQUENCY),
        Duration.ofMillis(HEARTBEAT_FREQUENCY),
        this::heartbeat);
    switch (context.descriptor().replication()) {
      case SYNCHRONOUS:
        replicator = new SynchronousReplicator(context);
        break;
      case ASYNCHRONOUS:
        replicator = new AsynchronousReplicator(context);
        break;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Applies a heartbeat to the service to ensure timers can be triggered.
   */
  private void heartbeat() {
    long index = context.nextIndex();
    WallClockTimestamp timestamp = context.nextTimestamp();
    replicator.replicate(new HeartbeatOperation(index, timestamp.unixTimestamp()))
        .thenRun(() -> context.service().tick(timestamp));
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    if (request.operation().id().type() == OperationType.COMMAND) {
      return executeCommand(request);
    } else if (request.operation().id().type() == OperationType.QUERY) {
      return executeQuery(request);
    }
    return Futures.exceptionalFuture(new IllegalArgumentException("Unknown operation type"));
  }

  private CompletableFuture<ExecuteResponse> executeCommand(ExecuteRequest request) {
    PrimaryBackupSession session = context.getOrCreateSession(request.session(), request.node());
    long index = context.nextIndex();
    WallClockTimestamp timestamp = context.nextTimestamp();
    return replicator.replicate(new ExecuteOperation(
        index,
        timestamp.unixTimestamp(),
        session.sessionId().id(),
        session.nodeId(),
        request.operation()))
        .thenApply(v -> {
          try {
            byte[] result = context.service().apply(new DefaultCommit<>(
                index,
                request.operation().id(),
                request.operation().value(),
                session,
                timestamp.unixTimestamp()));
            return ExecuteResponse.ok(result);
          } catch (Exception e) {
            return ExecuteResponse.error();
          }
        });
  }

  private CompletableFuture<ExecuteResponse> executeQuery(ExecuteRequest request) {
    Session session = context.getSession(request.session());
    if (session == null) {
      return CompletableFuture.completedFuture(ExecuteResponse.error());
    }

    try {
      byte[] result = context.service().apply(new DefaultCommit<>(
          context.getIndex(),
          request.operation().id(),
          request.operation().value(),
          session,
          context.currentTimestamp().unixTimestamp()));
      return CompletableFuture.completedFuture(ExecuteResponse.ok(result));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(ExecuteResponse.error());
    }
  }

  @Override
  public CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    logReceived(request);
    if (request.term() != context.currentTerm()) {
      return CompletableFuture.completedFuture(logResponse(RestoreResponse.error()));
    }

    HeapBuffer buffer = HeapBuffer.allocate();
    context.service().backup(buffer);
    buffer.flip();
    byte[] bytes = buffer.readBytes(buffer.remaining());
    return CompletableFuture.completedFuture(
        logResponse(RestoreResponse.ok(context.currentIndex(), context.currentTimestamp().unixTimestamp(), bytes)));
  }

  @Override
  public CompletableFuture<Void> expire(PrimaryBackupSession session) {
    long index = context.nextIndex();
    WallClockTimestamp timestamp = context.nextTimestamp();
    return replicator.replicate(new ExpireOperation(index, timestamp.unixTimestamp(), session.sessionId().id()))
        .thenRun(() -> context.sessions().expireSession(session));
  }

  @Override
  public CompletableFuture<Void> close(PrimaryBackupSession session) {
    long index = context.nextIndex();
    WallClockTimestamp timestamp = context.nextTimestamp();
    return replicator.replicate(new CloseOperation(index, timestamp.unixTimestamp(), session.sessionId().id()))
        .thenRun(() -> context.sessions().closeSession(session));
  }

  @Override
  public void close() {
    replicator.close();
    heartbeatTimer.cancel();
  }
}
