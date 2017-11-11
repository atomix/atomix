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

import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.FileBuffer;
import io.atomix.storage.buffer.HeapBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores information about a {@link Snapshot} of the state machine.
 * <p>
 * Snapshot descriptors represent the header of a snapshot file which stores metadata about
 * the snapshot contents. This API provides methods for reading and a builder for writing
 * snapshot headers/descriptors.
 */
public final class SnapshotDescriptor implements AutoCloseable {
  public static final int BYTES = 64;

  /**
   * Returns a descriptor builder.
   * <p>
   * The descriptor builder will write segment metadata to a {@code 48} byte in-memory buffer.
   *
   * @return The descriptor builder.
   */
  public static Builder builder() {
    return new Builder(HeapBuffer.allocate(BYTES));
  }

  /**
   * Returns a descriptor builder for the given descriptor buffer.
   *
   * @param buffer The descriptor buffer.
   * @return The descriptor builder.
   * @throws NullPointerException if {@code buffer} is null
   */
  public static Builder builder(Buffer buffer) {
    return new Builder(buffer);
  }

  private Buffer buffer;
  private final long serviceId;
  private final long index;
  private final long timestamp;
  private boolean locked;

  /**
   * @throws NullPointerException if {@code buffer} is null
   */
  public SnapshotDescriptor(Buffer buffer) {
    this.buffer = checkNotNull(buffer, "buffer cannot be null");
    this.serviceId = buffer.readLong();
    this.index = buffer.readLong();
    this.timestamp = buffer.readLong();
    this.locked = buffer.readBoolean();
    buffer.skip(BYTES - buffer.position());
  }

  /**
   * Returns the service identifier.
   *
   * @return The service identifier.
   */
  public long serviceId() {
    return serviceId;
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the snapshot timestamp.
   *
   * @return The snapshot timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns whether the snapshot has been locked by commitment.
   * <p>
   * A snapshot will be locked once it has been fully written.
   *
   * @return Indicates whether the snapshot has been locked.
   */
  public boolean isLocked() {
    return locked;
  }

  /**
   * Locks the segment.
   */
  public void lock() {
    buffer.flush()
        .writeBoolean(24, true)
        .flush();
    locked = true;
  }

  /**
   * Copies the snapshot to a new buffer.
   */
  SnapshotDescriptor copyTo(Buffer buffer) {
    this.buffer = buffer
        .writeLong(serviceId)
        .writeLong(index)
        .writeLong(timestamp)
        .writeBoolean(locked)
        .skip(BYTES - buffer.position())
        .flush();
    return this;
  }

  @Override
  public void close() {
    buffer.close();
  }

  /**
   * Deletes the descriptor.
   */
  public void delete() {
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    }
  }

  /**
   * Snapshot descriptor builder.
   */
  public static class Builder {
    private final Buffer buffer;

    private Builder(Buffer buffer) {
      this.buffer = checkNotNull(buffer, "buffer cannot be null");
    }

    /**
     * Sets the snapshot identifier.
     *
     * @param id The snapshot identifier.
     * @return The snapshot builder.
     */
    public Builder withServiceId(long id) {
      buffer.writeLong(0, id);
      return this;
    }

    /**
     * Sets the snapshot index.
     *
     * @param index The snapshot index.
     * @return The snapshot builder.
     */
    public Builder withIndex(long index) {
      buffer.writeLong(8, index);
      return this;
    }

    /**
     * Sets the snapshot timestamp.
     *
     * @param timestamp The snapshot timestamp.
     * @return The snapshot builder.
     */
    public Builder withTimestamp(long timestamp) {
      buffer.writeLong(16, timestamp);
      return this;
    }

    /**
     * Builds the snapshot descriptor.
     *
     * @return The built snapshot descriptor.
     */
    public SnapshotDescriptor build() {
      return new SnapshotDescriptor(buffer);
    }

  }

}
