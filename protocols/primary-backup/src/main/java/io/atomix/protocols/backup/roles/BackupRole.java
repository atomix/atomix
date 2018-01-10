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

import io.atomix.cluster.NodeId;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.CloseOperation;
import io.atomix.protocols.backup.protocol.ExecuteOperation;
import io.atomix.protocols.backup.protocol.ExpireOperation;
import io.atomix.protocols.backup.protocol.HeartbeatOperation;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.storage.buffer.HeapBuffer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Backup role.
 */
public class BackupRole extends PrimaryBackupRole {
  private final Queue<BackupOperation> operations = new LinkedList<>();

  public BackupRole(PrimaryBackupServiceContext service) {
    super(Role.BACKUP, service);
  }

  @Override
  public CompletableFuture<BackupResponse> backup(BackupRequest request) {
    logRequest(request);

    // If the term is greater than the node's current term, update the term.
    if (request.term() > context.currentTerm()) {
      context.resetTerm(request.term(), request.primary());
    }
    // If the term is less than the node's current term, ignore the backup message.
    else if (request.term() < context.currentTerm()) {
      return CompletableFuture.completedFuture(BackupResponse.error());
    }

    operations.addAll(request.operations());
    long currentCommitIndex = context.getCommitIndex();
    long nextCommitIndex = context.setCommitIndex(request.index());
    context.threadContext().execute(() -> applyOperations(currentCommitIndex, nextCommitIndex));
    return CompletableFuture.completedFuture(logResponse(BackupResponse.ok()));
  }

  /**
   * Applies operations in the given range.
   */
  private void applyOperations(long fromIndex, long toIndex) {
    for (long i = fromIndex + 1; i <= toIndex; i++) {
      BackupOperation operation = operations.poll();
      if (operation == null) {
        requestRestore(context.primary());
        break;
      }

      if (context.nextIndex(operation.index())) {
        switch (operation.type()) {
          case EXECUTE:
            applyExecute((ExecuteOperation) operation);
            break;
          case HEARTBEAT:
            applyHeartbeat((HeartbeatOperation) operation);
            break;
          case EXPIRE:
            applyExpire((ExpireOperation) operation);
            break;
          case CLOSE:
            applyClose((CloseOperation) operation);
            break;
        }
      } else {
        requestRestore(context.primary());
        break;
      }
    }
  }

  /**
   * Applies an execute operation to the service.
   */
  private void applyExecute(ExecuteOperation operation) {
    Session session = context.getOrCreateSession(operation.session(), operation.node());
    if (operation.operation() != null) {
      try {
        context.service().apply(new DefaultCommit<>(
            context.setIndex(operation.index()),
            operation.operation().id(),
            operation.operation().value(),
            context.setSession(session),
            context.setTimestamp(operation.timestamp())));
      } catch (Exception e) {
        log.warn("Failed to apply operation: {}", e);
      } finally {
        context.setSession(null);
      }
    }
  }

  /**
   * Applies a heartbeat operation to the service.
   */
  private void applyHeartbeat(HeartbeatOperation operation) {
    context.setTimestamp(operation.timestamp());
  }

  /**
   * Applies an expire operation.
   */
  private void applyExpire(ExpireOperation operation) {
    context.setTimestamp(operation.timestamp());
    PrimaryBackupSession session = context.getSession(operation.session());
    if (session != null) {
      context.sessions().expireSession(session);
    }
  }

  /**
   * Applies a close operation.
   */
  private void applyClose(CloseOperation operation) {
    context.setTimestamp(operation.timestamp());
    PrimaryBackupSession session = context.getSession(operation.session());
    if (session != null) {
      context.sessions().closeSession(session);
    }
  }

  /**
   * Requests a restore from the primary.
   */
  private void requestRestore(NodeId primary) {
    context.protocol().restore(primary, RestoreRequest.request(context.descriptor(), context.currentTerm()))
        .whenCompleteAsync((response, error) -> {
          if (error == null) {
            context.resetIndex(response.index(), response.timestamp());
            context.service().restore(HeapBuffer.wrap(response.data()));
            operations.clear();
          }
        }, context.threadContext());
  }
}
