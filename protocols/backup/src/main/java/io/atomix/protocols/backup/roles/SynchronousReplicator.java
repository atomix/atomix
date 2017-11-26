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
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Synchronous replicator.
 */
class SynchronousReplicator implements Replicator {
  private final PrimaryBackupServiceContext context;
  private final Map<NodeId, BackupQueue> queues = new HashMap<>();
  private final Queue<CompletableFuture<Void>> futures = new LinkedList<>();
  private long completeIndex;

  SynchronousReplicator(PrimaryBackupServiceContext context) {
    this.context = context;
  }

  @Override
  public CompletableFuture<Void> replicate(BackupOperation operation) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    futures.add(future);
    for (NodeId backup : context.backups()) {
      queues.computeIfAbsent(backup, BackupQueue::new).add(operation);
    }
    return future;
  }

  /**
   * Completes futures.
   */
  private void completeFutures() {
    long commitIndex = queues.values().stream()
        .map(queue -> queue.ackedIndex)
        .reduce(Math::min)
        .orElse(0L);
    for (long i = completeIndex + 1; i <= commitIndex; i++) {
      futures.remove().complete(null);
    }
    this.completeIndex = commitIndex;
  }

  @Override
  public void close() {
    futures.forEach(f -> f.completeExceptionally(new IllegalStateException("Not the primary")));
  }

  /**
   * Synchronous backup queue.
   */
  private final class BackupQueue {
    private final List<BackupOperation> operations = new LinkedList<>();
    private final NodeId nodeId;
    private boolean inProgress;
    private long ackedIndex;

    BackupQueue(NodeId nodeId) {
      this.nodeId = nodeId;
    }

    void add(BackupOperation operation) {
      operations.add(operation);
      maybeBackup();
    }

    private void maybeBackup() {
      if (!inProgress && !operations.isEmpty()) {
        inProgress = true;
        backup();
      }
    }

    private void backup() {
      List<BackupOperation> operations = this.operations.subList(0, Math.min(100, this.operations.size()));
      long lastIndex = operations.get(operations.size() - 1).index();
      BackupRequest request = BackupRequest.request(context.descriptor(), context.nodeId(), context.currentTerm(), completeIndex, operations);
      context.protocol().backup(nodeId, request).whenCompleteAsync((response, error) -> {
        if (error == null && response.status() == Status.OK) {
          ackedIndex = lastIndex;
          completeFutures();
        }
        inProgress = false;
        maybeBackup();
      }, context.threadContext());
      operations.clear();
    }
  }
}
