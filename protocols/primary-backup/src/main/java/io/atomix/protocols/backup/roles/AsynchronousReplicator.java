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

import com.google.common.collect.ImmutableList;
import io.atomix.cluster.NodeId;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous replicator.
 */
class AsynchronousReplicator implements Replicator {
  private static final int MAX_BATCH_SIZE = 100;
  private static final long MAX_BATCH_TIME = 100;

  private final PrimaryBackupServiceContext context;
  private final Logger log;
  private final Map<NodeId, BackupQueue> queues = new HashMap<>();

  AsynchronousReplicator(PrimaryBackupServiceContext context, Logger log) {
    this.context = context;
    this.log = log;
  }

  @Override
  public CompletableFuture<Void> replicate(BackupOperation operation) {
    List<CompletableFuture<Void>> futures = new ArrayList<>(context.backups().size());
    for (NodeId backup : context.backups()) {
      futures.add(queues.computeIfAbsent(backup, BackupQueue::new).add(operation));
    }
    context.setCommitIndex(operation.index());
    return Futures.allOf(futures).thenApply(v -> null);
  }

  @Override
  public void removePreviousOperation(NodeId nodeId, long endIndex) {
    queues.computeIfPresent(nodeId, (node, queue) -> {
      queue.clear(endIndex);
      return queue;
    });
  }

  @Override
  public void close() {
    queues.values().forEach(BackupQueue::close);
  }

  /**
   * Asynchronous backup queue.
   */
  private final class BackupQueue {
    private final Queue<BackupOperation> operations = new LinkedList<>();
    private final NodeId nodeId;
    private final Scheduled backupTimer;
    private CompletableFuture<Void> backupFuture = CompletableFuture.completedFuture(null);
    private long lastSent;

    BackupQueue(NodeId nodeId) {
      this.nodeId = nodeId;
      this.backupTimer = context.threadContext()
          .schedule(Duration.ofMillis(MAX_BATCH_TIME / 2), Duration.ofMillis(MAX_BATCH_TIME / 2), this::maybeBackup);
    }

    /**
     * Adds an operation to the queue.
     *
     * @param operation the operation to add
     */
    CompletableFuture<Void> add(BackupOperation operation) {
      operations.add(operation);
      if (operations.size() >= MAX_BATCH_SIZE && backupFuture.isDone()) {
        return backupFuture = backup();
      }
      return CompletableFuture.completedFuture(null);
    }

    /**
     * Sends the next batch if enough time has elapsed.
     */
    private void maybeBackup() {
      if (System.currentTimeMillis() - lastSent > MAX_BATCH_TIME && !operations.isEmpty() && backupFuture.isDone()) {
        backupFuture = backup();
      }
    }

    /**
     * Sends the next batch to the backup.
     */
    private CompletableFuture<Void> backup() {
      List<BackupOperation> batch = ImmutableList.copyOf(operations);
      operations.clear();
      BackupRequest request = BackupRequest.request(
          context.descriptor(),
          context.nodeId(),
          context.currentTerm(),
          context.currentIndex(),
          batch);
      log.trace("Sending {} to {}", request, nodeId);
      return context.protocol().backup(nodeId, request).thenRun(() -> lastSent = System.currentTimeMillis());
    }

    /**
     * Closes the queue.
     */
    void close() {
      backupTimer.cancel();
    }


    /**
     * Clears the queue.
     */
    void clear(long index) {
      BackupOperation op = operations.peek();
      while (op != null && op.index() <= index) {
        operations.remove();
        op = operations.peek();
      }
    }
  }
}
