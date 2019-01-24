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
 * limitations under the License
 */
package io.atomix.protocols.raft.storage.snapshot;

import io.atomix.utils.time.WallClockTimestamp;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Manages reading and writing a single snapshot file.
 * <p>
 * User-provided state machines which implement the {@link Snapshottable} interface
 * transparently write snapshots to and read snapshots from files on disk. Each time a snapshot is taken of
 * the state machine state, the snapshot will be written to a single file represented by this interface.
 * Snapshots are backed by a {@link io.atomix.storage.buffer.Buffer} dictated by the parent
 * {@link io.atomix.storage.StorageLevel} configuration. Snapshots for file-based storage
 * levels like {@link io.atomix.storage.StorageLevel#DISK DISK} will be stored in a disk
 * backed buffer, and {@link io.atomix.storage.StorageLevel#MEMORY MEMORY} snapshots will
 * be stored in an on-heap buffer.
 * <p>
 * Snapshots are read and written by a {@link SnapshotReader} and {@link SnapshotWriter} respectively.
 * To create a reader or writer, use the {@link #openReader()} and {@link #openWriter()} methods.
 * <p>
 * <pre>
 *   {@code
 *   Snapshot snapshot = snapshotStore.snapshot(1);
 *   try (SnapshotWriter writer = snapshot.writer()) {
 *     writer.writeString("Hello world!");
 *   }
 *   snapshot.complete();
 *   }
 * </pre>
 * A {@link SnapshotReader} is not allowed to be created until a {@link SnapshotWriter} has
 * completed writing the snapshot file and the snapshot has been marked {@link #complete() complete}.
 * This allows snapshots to effectively be written and closed but not completed until other conditions
 * are met. Prior to the completion of a snapshot, a failure and recovery of the parent {@link SnapshotStore}
 * will <em>not</em> recover an incomplete snapshot. Once a snapshot is complete, the snapshot becomes immutable,
 * can be recovered after a failure, and can be read by multiple readers concurrently.
 */
public abstract class Snapshot implements AutoCloseable {
  protected final SnapshotDescriptor descriptor;
  protected final SnapshotStore store;
  private SnapshotWriter writer;

  protected Snapshot(SnapshotDescriptor descriptor, SnapshotStore store) {
    this.descriptor = checkNotNull(descriptor, "descriptor cannot be null");
    this.store = checkNotNull(store, "store cannot be null");
  }

  /**
   * Returns the snapshot index.
   * <p>
   * The snapshot index is the index of the state machine at the point at which the snapshot was written.
   *
   * @return The snapshot index.
   */
  public long index() {
    return descriptor.index();
  }

  /**
   * Returns the snapshot timestamp.
   * <p>
   * The timestamp is the wall clock time at the {@link #index()} at which the snapshot was taken.
   *
   * @return The snapshot timestamp.
   */
  public WallClockTimestamp timestamp() {
    return WallClockTimestamp.from(descriptor.timestamp());
  }

  /**
   * Returns the snapshot format version.
   *
   * @return the snapshot format version
   */
  public int version() {
    return descriptor.version();
  }

  /**
   * Opens a new snapshot writer.
   * <p>
   * Only a single {@link SnapshotWriter} per {@link Snapshot} can be created. The single writer
   * must write the snapshot in full and {@link #complete()} the snapshot to persist it to disk
   * and make it available for {@link #openReader() reads}.
   *
   * @return A new snapshot writer.
   * @throws IllegalStateException if a writer was already created or the snapshot is {@link #complete() complete}
   */
  public abstract SnapshotWriter openWriter();

  /**
   * Checks that the snapshot can be written.
   */
  protected void checkWriter() {
    checkState(writer == null, "cannot create multiple writers for the same snapshot");
  }

  /**
   * Opens the given snapshot writer.
   */
  protected SnapshotWriter openWriter(SnapshotWriter writer, SnapshotDescriptor descriptor) {
    checkWriter();
    checkState(!descriptor.isLocked(), "cannot write to locked snapshot descriptor");
    this.writer = checkNotNull(writer, "writer cannot be null");
    return writer;
  }

  /**
   * Closes the current snapshot writer.
   */
  protected void closeWriter(SnapshotWriter writer) {
    this.writer = null;
  }

  /**
   * Opens a new snapshot reader.
   * <p>
   * A {@link SnapshotReader} can only be created for a snapshot that has been fully written and
   * {@link #complete() completed}. Multiple concurrent readers can be created for the same snapshot
   * since completed snapshots are immutable.
   *
   * @return A new snapshot reader.
   * @throws IllegalStateException if the snapshot is not {@link #complete() complete}
   */
  public abstract SnapshotReader openReader();

  /**
   * Opens the given snapshot reader.
   */
  protected SnapshotReader openReader(SnapshotReader reader, SnapshotDescriptor descriptor) {
    checkState(descriptor.isLocked(), "cannot read from unlocked snapshot descriptor");
    return reader;
  }

  /**
   * Closes the current snapshot reader.
   */
  protected void closeReader(SnapshotReader reader) {

  }

  /**
   * Completes writing the snapshot to persist it and make it available for reads.
   * <p>
   * Snapshot writers must call this method to persist a snapshot to disk. Prior to completing a
   * snapshot, failure and recovery of the parent {@link SnapshotStore} will not result in recovery
   * of this snapshot. Additionally, no {@link #openReader() readers} can be created until the snapshot
   * has been completed.
   *
   * @return The completed snapshot.
   */
  public Snapshot complete() {
    store.completeSnapshot(this);
    return this;
  }

  /**
   * Persists the snapshot to disk if necessary.
   * <p>
   * If the snapshot store is backed by disk, the snapshot will be persisted.
   *
   * @return The persisted snapshot.
   */
  public Snapshot persist() {
    return this;
  }

  /**
   * Returns whether the snapshot is persisted.
   *
   * @return Whether the snapshot is persisted.
   */
  public abstract boolean isPersisted();

  /**
   * Closes the snapshot.
   */
  @Override
  public void close() {
  }

  /**
   * Deletes the snapshot.
   */
  public void delete() {
  }

  @Override
  public int hashCode() {
    return Objects.hash(index());
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Snapshot snapshot = (Snapshot) object;
    return snapshot.index() == index();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .toString();
  }
}
