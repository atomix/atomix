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
package io.atomix.protocols.raft.storage.compactor;

import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Raft log compactor.
 */
public class RaftLogCompactor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLogCompactor.class);

  private static final Duration SNAPSHOT_INTERVAL = Duration.ofSeconds(10);
  private static final Duration MIN_COMPACT_INTERVAL = Duration.ofSeconds(10);

  private static final int SEGMENT_BUFFER_FACTOR = 5;

  private final RaftContext raft;
  private final ThreadContext threadContext;
  private final Random random = new Random();
  private volatile CompletableFuture<Void> compactFuture;
  private long lastCompacted;

  public RaftLogCompactor(RaftContext raft, ThreadContext threadContext) {
    this.raft = raft;
    this.threadContext = threadContext;
    scheduleSnapshots();
  }

  /**
   * Returns a boolean indicating whether the node is running out of disk space.
   */
  private boolean isRunningOutOfDiskSpace() {
    // If there's not enough space left to allocate two log segments
    return raft.getStorage().statistics().getUsableSpace() < raft.getStorage().maxLogSegmentSize() * SEGMENT_BUFFER_FACTOR
        // Or the used disk percentage has surpassed the free disk buffer percentage
        || raft.getStorage().statistics().getUsableSpace() / (double) raft.getStorage().statistics().getTotalSpace() < raft.getStorage().freeDiskBuffer();
  }

  /**
   * Schedules a snapshot iteration.
   */
  private void scheduleSnapshots() {
    threadContext.schedule(SNAPSHOT_INTERVAL, () -> snapshotServices(true, false));
  }

  /**
   * Compacts Raft logs.
   *
   * @return a future to be completed once logs have been compacted
   */
  public CompletableFuture<Void> compact() {
    return snapshotServices(false, true);
  }

  /**
   * Takes a snapshot of all services and compacts logs if the server is not under high load or disk needs to be freed.
   */
  private synchronized CompletableFuture<Void> snapshotServices(boolean rescheduleAfterCompletion, boolean force) {
    // If compaction is already in progress, return the existing future and reschedule if this is a scheduled compaction.
    if (compactFuture != null) {
      if (rescheduleAfterCompletion) {
        compactFuture.whenComplete((r, e) -> scheduleSnapshots());
      }
      return compactFuture;
    }

    long lastApplied = raft.getLastApplied();

    // Only take snapshots if segments can be removed from the log below the lastApplied index.
    if (raft.getLog().isCompactable(lastApplied) && raft.getLog().getCompactableIndex(lastApplied) > lastCompacted) {

      // Determine whether the node is running out of disk space.
      boolean runningOutOfDiskSpace = isRunningOutOfDiskSpace();

      // If compaction is not already being forced...
      if (!force
          // And the log is not in memory (we need to free up memory if it is)...
          && raft.getStorage().storageLevel() != StorageLevel.MEMORY
          // And dynamic compaction is enabled (we need to compact immediately if it's disabled)...
          && raft.getStorage().dynamicCompaction()
          // And the node isn't running out of disk space (we need to compact immediately if it is)...
          && !runningOutOfDiskSpace
          // And the server is under high load (we can skip compaction at this point)...
          && raft.getLoadMonitor().isUnderHighLoad()) {
        // We can skip taking a snapshot for now.
        LOGGER.debug("Skipping compaction due to high load");
        if (rescheduleAfterCompletion) {
          scheduleSnapshots();
        }
        return CompletableFuture.completedFuture(null);
      }

      LOGGER.debug("Snapshotting services");

      // Update the index at which the log was last compacted.
      this.lastCompacted = lastApplied;

      // Copy the set of services. We don't need to account for new services that are created during the
      // snapshot/compaction process since we're only deleting segments prior to the creation of all
      // services that existed at the start of compaction.
      List<RaftServiceContext> services = new ArrayList<>(raft.getServices().copyValues());

      // We need to ensure that callbacks added to the compaction future are completed in the order in which they
      // were added in order to preserve the order of retries when appending to the log.
      compactFuture = new OrderedFuture<>();

      // Wait for snapshots in all state machines to be completed before compacting the log at the last applied index.
      snapshotServices(services, lastApplied, force || runningOutOfDiskSpace)
          .whenComplete((result, error) -> {
            // If log compaction is being forced, immediately compact the logs.
            if (force) {
              compactLogs(lastApplied);
            } else {
              scheduleCompaction(lastApplied);
            }
          });

      // Reschedule snapshots after completion if necessary.
      if (rescheduleAfterCompletion) {
        compactFuture.whenComplete((r, e) -> scheduleSnapshots());
      }
      return compactFuture;
    }
    // Otherwise, if the log can't be compacted anyways, just reschedule snapshots.
    else {
      if (rescheduleAfterCompletion) {
        scheduleSnapshots();
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Takes and persists snapshots of provided services.
   *
   * @param services a list of services to snapshot
   * @param index the compaction index
   * @param force whether to force snapshotting all services to free disk space
   * @return future to be completed once all snapshots have been completed
   */
  private CompletableFuture<Void> snapshotServices(List<RaftServiceContext> services, long index, boolean force) {
    return snapshotServices(services, index, force, 0, new ArrayList<>());
  }

  /**
   * Takes and persists snapshots of provided services.
   * <p>
   * Snapshots are attempted on services that are not experiencing high load. In the event there are no more services
   * that can be snapshotted, an attempt will be scheduled again for the future using exponential backoff.
   *
   * @param services a list of services to snapshot
   * @param index the compaction index
   * @param force whether to force snapshotting all services to free disk space
   * @param attempt the current attempt count
   * @param futures reference to a list of futures for all service snapshots
   * @return future to be completed once all snapshots have been completed
   */
  private CompletableFuture<Void> snapshotServices(
      List<RaftServiceContext> services,
      long index,
      boolean force,
      int attempt,
      List<CompletableFuture<Void>> futures) {
    // If all services have been processed, return a successfully completed future.
    if (services.isEmpty()) {
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    // Select any service that can be snapshotted.
    RaftServiceContext nextService = selectService(services, force);

    if (nextService != null) {
      // Take a snapshot and then persist the snapshot after some interval. This is done to avoid persisting snapshots
      // too close to the head of a follower's log such that the snapshot will be replicated in place of entries.
      futures.add(nextService.takeSnapshot(index)
          .thenCompose(snapshotIndex -> {
            // If compaction need to be completed immediately, complete the snapshot immediately.
            if (force) {
              return nextService.completeSnapshot(snapshotIndex);
            }
            // Otherwise, schedule the completion in the future to allow time for followers to advance beyond the snapshot.
            else {
              return scheduleCompletion(nextService, snapshotIndex);
            }
          }));

      // Recursively snapshot remaining services, resetting the attempt count.
      return snapshotServices(services, index, force, 0, futures);
    } else {
      return rescheduleSnapshots(services, index, force, attempt, futures);
    }
  }

  /**
   * Reschedules an attempt to snapshot remaining services.
   * <p>
   * When a snapshot cannot be taken of any remaining service, snapshots are rescheduled using exponential backoff based
   * on the {@code attempt} count.
   *
   * @param services a list of services to snapshot
   * @param index the compaction index
   * @param attempt the current attempt count
   * @param futures reference to a list of futures for all service snapshots
   * @return future to be completed once all snapshots have been completed
   */
  private CompletableFuture<Void> rescheduleSnapshots(
      List<RaftServiceContext> services,
      long index,
      boolean force,
      int attempt,
      List<CompletableFuture<Void>> futures) {
    ComposableFuture<Void> future = new ComposableFuture<>();
    threadContext.schedule(Duration.ofSeconds(Math.min(2 ^ attempt, 10)), () ->
        snapshotServices(services, index, force || isRunningOutOfDiskSpace(), attempt + 1, futures).whenComplete(future));
    return future;
  }

  /**
   * Selects the next service to snapshot.
   * <p>
   * Services that are not under high load are selected unless compaction is being forced by low available disk space.
   * When a service is selected, it will be removed from the {@code services} list reference and returned. If no
   * service can be snapshotted, returns {@code null}.
   *
   * @param services a list of services from which to select a service
   * @param force whether to force snapshotting all services to free disk space
   * @return the service to snapshot or {@code null} if no service can be snapshotted
   */
  private RaftServiceContext selectService(List<RaftServiceContext> services, boolean force) {
    Iterator<RaftServiceContext> iterator = services.iterator();
    while (iterator.hasNext()) {
      RaftServiceContext serviceContext = iterator.next();
      if (force || !raft.getStorage().dynamicCompaction() || !serviceContext.isUnderHighLoad()) {
        iterator.remove();
        return serviceContext;
      }
    }
    return null;
  }

  /**
   * Schedules completion of a snapshot after a randomized delay to reduce the chance the snapshot will need to be
   * replicated to followers.
   *
   * @param serviceContext the service for which to complete the snapshot
   * @param snapshotIndex the index of the snapshot
   * @return future to be completed once the snapshot has been completed
   */
  private CompletableFuture<Void> scheduleCompletion(RaftServiceContext serviceContext, long snapshotIndex) {
    ComposableFuture<Void> future = new ComposableFuture<>();
    Duration delay = SNAPSHOT_INTERVAL.plusMillis(random.nextInt((int) SNAPSHOT_INTERVAL.toMillis()));
    threadContext.schedule(delay, () -> serviceContext.completeSnapshot(snapshotIndex).whenComplete(future));
    return future;
  }

  /**
   * Schedules a log compaction.
   *
   * @param lastApplied the last applied index at the start of snapshotting. This represents the highest index before
   *                    which segments can be safely removed from disk
   */
  private void scheduleCompaction(long lastApplied) {
    // Schedule compaction after a randomized delay to discourage snapshots on multiple nodes at the same time.
    Duration delay = MIN_COMPACT_INTERVAL.plusMillis(random.nextInt((int) MIN_COMPACT_INTERVAL.toMillis()));
    LOGGER.trace("Scheduling compaction in {}", delay);
    threadContext.schedule(delay, () -> compactLogs(lastApplied));
  }

  /**
   * Compacts logs up to the given index.
   *
   * @param compactIndex the index to which to compact logs
   */
  private void compactLogs(long compactIndex) {
    LOGGER.debug("Compacting logs up to index {}", compactIndex);
    try {
      raft.getLog().compact(compactIndex);
    } catch (Exception e) {
      LOGGER.error("An exception occurred during log compaction: {}", e);
    } finally {
      this.compactFuture.complete(null);
      this.compactFuture = null;
      // Immediately attempt to take new snapshots since compaction is already run after a time interval.
      compact();
    }
  }
}
