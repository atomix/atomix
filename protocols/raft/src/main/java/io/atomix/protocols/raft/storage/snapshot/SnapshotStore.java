/*
 * Copyright 2015-present Open Networking Laboratory
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
 * limitations under the License
 */
package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.buffer.FileBuffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.time.WallClockTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Persists server snapshots via the {@link RaftStorage} module.
 * <p>
 * The server snapshot store is responsible for persisting periodic state machine snapshots according
 * to the configured {@link RaftStorage#storageLevel() storage level}. Each server with a snapshottable state machine
 * persists the state machine state to allow commands to be removed from disk.
 * <p>
 * When a snapshot store is {@link RaftStorage#openSnapshotStore() created}, the store will load any
 * existing snapshots from disk and make them available for reading. Only snapshots that have been
 * written and {@link Snapshot#complete() completed} will be read from disk. Incomplete snapshots are
 * automatically deleted from disk when the snapshot store is opened.
 * <p>
 * <pre>
 *   {@code
 *   SnapshotStore snapshots = storage.openSnapshotStore("test");
 *   Snapshot snapshot = snapshots.snapshot(1);
 *   }
 * </pre>
 * To create a new {@link Snapshot}, use the {@link #newSnapshot(ServiceId, long, WallClockTimestamp)} method. Each snapshot must
 * be created with a unique {@code index} which represents the index of the server state machine at
 * the point at which the snapshot was taken. Snapshot indices are used to sort snapshots loaded from
 * disk and apply them at the correct point in the state machine.
 * <p>
 * <pre>
 *   {@code
 *   Snapshot snapshot = snapshots.create(10);
 *   try (SnapshotWriter writer = snapshot.writer()) {
 *     ...
 *   }
 *   snapshot.complete();
 *   }
 * </pre>
 * Snapshots don't necessarily represent the beginning of the log. Typical Raft implementations take a
 * snapshot of the state machine state and then clear their logs up to that point. However, in Raft
 * a snapshot may actually only represent a subset of the state machine's state.
 */
public class SnapshotStore implements AutoCloseable {
  private final Logger log = LoggerFactory.getLogger(getClass());
  final RaftStorage storage;
  private final Map<Long, Snapshot> indexSnapshots = new ConcurrentHashMap<>();
  private final Map<ServiceId, Snapshot> stateMachineSnapshots = new ConcurrentHashMap<>();

  public SnapshotStore(RaftStorage storage) {
    this.storage = checkNotNull(storage, "storage cannot be null");
    open();
  }

  /**
   * Opens the snapshot manager.
   */
  private void open() {
    for (Snapshot snapshot : loadSnapshots()) {
      Snapshot existingSnapshot = stateMachineSnapshots.get(snapshot.serviceId());
      if (existingSnapshot == null || existingSnapshot.index() < snapshot.index()) {
        stateMachineSnapshots.put(snapshot.serviceId(), snapshot);

        // If a newer snapshot was found, delete the old snapshot if necessary.
        if (existingSnapshot != null && !storage.isRetainStaleSnapshots()) {
          existingSnapshot.close();
          existingSnapshot.delete();
        }
      }
    }

    for (Snapshot snapshot : stateMachineSnapshots.values()) {
      indexSnapshots.put(snapshot.index(), snapshot);
    }
  }

  /**
   * Returns the last snapshot for the given state machine identifier.
   *
   * @param id The state machine identifier for which to return the snapshot.
   * @return The latest snapshot for the given state machine.
   */
  public Snapshot getSnapshotById(ServiceId id) {
    return stateMachineSnapshots.get(id);
  }

  /**
   * Returns the snapshot at the given index.
   *
   * @param index The index for which to return the snapshot.
   * @return The snapshot at the given index.
   */
  public Snapshot getSnapshotByIndex(long index) {
    return indexSnapshots.get(index);
  }

  /**
   * Loads all available snapshots from disk.
   *
   * @return A list of available snapshots.
   */
  private Collection<Snapshot> loadSnapshots() {
    // Ensure log directories are created.
    storage.directory().mkdirs();

    List<Snapshot> snapshots = new ArrayList<>();

    // Iterate through all files in the log directory.
    for (File file : storage.directory().listFiles(File::isFile)) {

      // If the file looks like a segment file, attempt to load the segment.
      if (SnapshotFile.isSnapshotFile(storage.prefix(), file)) {
        SnapshotFile snapshotFile = new SnapshotFile(file);
        SnapshotDescriptor descriptor = new SnapshotDescriptor(FileBuffer.allocate(file, SnapshotDescriptor.BYTES));

        // Valid segments will have been locked. Segments that resulting from failures during log cleaning will be
        // unlocked and should ultimately be deleted from disk.
        if (descriptor.isLocked()) {
          log.debug("Loaded disk snapshot: {} ({})", snapshotFile.index(), snapshotFile.file().getName());
          snapshots.add(new FileSnapshot(snapshotFile, this));
          descriptor.close();
        }
        // If the segment descriptor wasn't locked, close and delete the descriptor.
        else {
          log.debug("Deleting partial snapshot: {} ({})", descriptor.index(), snapshotFile.file().getName());
          descriptor.close();
          descriptor.delete();
        }
      }
    }

    return snapshots;
  }

  /**
   * Creates a temporary in-memory snapshot.
   *
   * @param serviceId The snapshot identifier.
   * @param index The snapshot index.
   * @param timestamp The snapshot timestamp.
   * @return The snapshot.
   */
  public Snapshot newTemporarySnapshot(ServiceId serviceId, long index, WallClockTimestamp timestamp) {
    SnapshotDescriptor descriptor = SnapshotDescriptor.newBuilder()
        .withId(serviceId.id())
        .withIndex(index)
        .withTimestamp(timestamp.unixTimestamp())
        .build();
    return newSnapshot(descriptor, StorageLevel.MEMORY);
  }

  /**
   * Creates a new snapshot.
   *
   * @param serviceId The snapshot identifier.
   * @param index The snapshot index.
   * @param timestamp The snapshot timestamp.
   * @return The snapshot.
   */
  public Snapshot newSnapshot(ServiceId serviceId, long index, WallClockTimestamp timestamp) {
    SnapshotDescriptor descriptor = SnapshotDescriptor.newBuilder()
        .withId(serviceId.id())
        .withIndex(index)
        .withTimestamp(timestamp.unixTimestamp())
        .build();
    return newSnapshot(descriptor, storage.storageLevel());
  }

  /**
   * Creates a new snapshot buffer.
   */
  private Snapshot newSnapshot(SnapshotDescriptor descriptor, StorageLevel storageLevel) {
    if (storageLevel == StorageLevel.MEMORY) {
      return createMemorySnapshot(descriptor);
    } else {
      return createDiskSnapshot(descriptor);
    }
  }

  /**
   * Creates a memory snapshot.
   */
  private Snapshot createMemorySnapshot(SnapshotDescriptor descriptor) {
    HeapBuffer buffer = HeapBuffer.allocate(SnapshotDescriptor.BYTES, Integer.MAX_VALUE);
    Snapshot snapshot = new MemorySnapshot(buffer, descriptor.copyTo(buffer), this);
    log.debug("Created memory snapshot: {}", snapshot);
    return snapshot;
  }

  /**
   * Creates a disk snapshot.
   */
  private Snapshot createDiskSnapshot(SnapshotDescriptor descriptor) {
    SnapshotFile file = new SnapshotFile(SnapshotFile.createSnapshotFile(storage.prefix(), storage.directory(), descriptor.snapshotId(), descriptor.index(), descriptor.timestamp()));
    Snapshot snapshot = new FileSnapshot(file, this);
    log.debug("Created disk snapshot: {}", snapshot);
    return snapshot;
  }

  /**
   * Completes writing a snapshot.
   */
  protected synchronized void completeSnapshot(Snapshot snapshot) {
    checkNotNull(snapshot, "snapshot cannot be null");

    // Only store the snapshot if no existing snapshot exists.
    Snapshot existingSnapshot = stateMachineSnapshots.get(snapshot.serviceId());
    if (existingSnapshot == null || existingSnapshot.index() <= snapshot.index()) {
      stateMachineSnapshots.put(snapshot.serviceId(), snapshot);
      indexSnapshots.put(snapshot.index(), snapshot);

      // Delete the old snapshot if necessary.
      if (existingSnapshot != null) {
        indexSnapshots.remove(existingSnapshot.index());
        if (!storage.isRetainStaleSnapshots()) {
          existingSnapshot.close();
          existingSnapshot.delete();
        }
      }
    }
    // If the snapshot was old, delete it if necessary.
    else if (!storage.isRetainStaleSnapshots()) {
      snapshot.close();
      snapshot.delete();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
