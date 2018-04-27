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
import io.atomix.cluster.MemberId;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.concurrent.Scheduled;
import org.slf4j.Logger;

import java.time.Duration;
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
  private final Map<MemberId, BackupQueue> queues = new HashMap<>();

  AsynchronousReplicator(PrimaryBackupServiceContext context, Logger log) {
    this.context = context;
    this.log = log;
  }

  @Override
  public CompletableFuture<Void> replicate(BackupOperation operation) {
    for (MemberId backup : context.backups()) {
      queues.computeIfAbsent(backup, BackupQueue::new).add(operation);
    }
    context.setCommitIndex(operation.index());
    return CompletableFuture.completedFuture(null);
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
    private final MemberId memberId;
    private final Scheduled backupTimer;
    private long lastSent;

    BackupQueue(MemberId memberId) {
      this.memberId = memberId;
      this.backupTimer = context.threadContext()
          .schedule(Duration.ofMillis(MAX_BATCH_TIME / 2), Duration.ofMillis(MAX_BATCH_TIME / 2), this::maybeBackup);
    }

    /**
     * Adds an operation to the queue.
     *
     * @param operation the operation to add
     */
    void add(BackupOperation operation) {
      operations.add(operation);
      if (operations.size() >= MAX_BATCH_SIZE) {
        backup();
      }
    }

    /**
     * Sends the next batch if enough time has elapsed.
     */
    private void maybeBackup() {
      if (System.currentTimeMillis() - lastSent > MAX_BATCH_TIME && !operations.isEmpty()) {
        backup();
      }
    }

    /**
     * Sends the next batch to the backup.
     */
    private void backup() {
      List<BackupOperation> batch = ImmutableList.copyOf(operations);
      operations.clear();
      BackupRequest request = BackupRequest.request(
          context.descriptor(),
          context.memberId(),
          context.currentTerm(),
          context.currentIndex(),
          batch);
      log.trace("Sending {} to {}", request, memberId);
      context.protocol().backup(memberId, request);
      lastSent = System.currentTimeMillis();
    }

    /**
     * Closes the queue.
     */
    void close() {
      backupTimer.cancel();
    }
  }
}
