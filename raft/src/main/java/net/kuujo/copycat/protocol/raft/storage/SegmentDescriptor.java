/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.FileBuffer;
import net.kuujo.copycat.io.HeapBuffer;

/**
 * Segment descriptor.
 * <p>
 * The segment descriptor manages metadata related to a single segment of the log. Descriptors are stored within the
 * first {@code 48} bytes of each segment in the following order:
 * - {@code id} (64-bit signed integer) - A unique segment identifier. This is a monotonically increasing number within
 *   each log. Segments with in-sequence identifiers should contain in-sequence indexes.
 * - {@code index} (64-bit signed integer) - The effective first index of the segment. This indicates the index at which
 *   the first entry should be written to the segment. Indexes are monotonically increasing thereafter.
 * - {@code range} (64-bit signed integer) - The effective length of the segment. Regardless of the actual number of
 *   entries in the segment, the range indicates the total number of allowed entries within each segment. If a segment's
 *   index is {@code 1} and its range is {@code 10} then the next segment should start at index {@code 11}.
 * - {@code version} (64-bit signed integer) - The version of the segment. Versions are monotonically increasing
 *   starting at {@code 1}. Versions will only be incremented whenever the segment is rewritten to another memory/disk
 *   space, e.g. after log compaction.
 * - {@code updated} (64-bit signed integer) - The last update to the segment in terms of milliseconds since the epoch.
 *   When the segment is first constructed, the {@code updated} time is {@code 0}. Once all entries in the segment have
 *   been committed, the {@code updated} time should be set to the current time. Log compaction should not result in a
 *   change to {@code updated}.
 * - {@code maxEntrySize} (32-bit signed integer) - The maximum length in bytes of entry values allowed by the segment.
 * - {@code entries} (32-bit signed integer) - The total number of expected entries in the segment. This is the final
 *   number of entries allowed within the segment both before and after compaction. This entry count is used to determine
 *   the size of internal indexing and deduplication facilities.
 * - {@code locked} (8-bit boolean) - A boolean indicating whether the segment is locked. Segments will be locked once
 *   all entries have been committed to the segment. The lock state of each segment is used to determine log compaction
 *   and recovery behavior.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class SegmentDescriptor implements AutoCloseable {
  public static final int BYTES = 48;

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
   */
  public static Builder builder(Buffer buffer) {
    return new Builder(buffer);
  }

  private Buffer buffer;
  private final long id;
  private final long index;
  private final long range;
  private final long version;
  private long updated;
  private final int maxEntrySize;
  private final int maxSegmentSize;
  private boolean locked;

  public SegmentDescriptor(Buffer buffer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    this.buffer = buffer;
    this.id = buffer.readLong();
    this.version = buffer.readLong();
    this.index = buffer.readLong();
    this.range = buffer.readInt();
    this.maxEntrySize = buffer.readUnsignedMedium();
    this.maxSegmentSize = buffer.readInt();
    this.updated = buffer.readLong();
    this.locked = buffer.readBoolean();
  }

  /**
   * Returns the segment identifier.
   * <p>
   * The segment ID is a monotonically increasing number within each log. Segments with in-sequence identifiers should
   * contain in-sequence indexes.
   *
   * @return The segment identifier.
   */
  public long id() {
    return id;
  }

  /**
   * Returns the segment version.
   * <p>
   * Versions are monotonically increasing starting at {@code 1}. Versions will only be incremented whenever the segment
   * is rewritten to another memory/disk space, e.g. after log compaction.
   *
   * @return The segment version.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the segment index.
   * <p>
   * The index indicates the index at which the first entry should be written to the segment. Indexes are monotonically
   * increasing thereafter.
   *
   * @return The segment index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the total number of possible entries in the segment.
   * <p>
   * Regardless of the actual number of {@code entries} in the segment, the range indicates the total number of allowed
   * entries within each segment. If a segment's index is {@code 1} and its range is {@code 10} then the next segment
   * should start at index {@code 11}.
   *
   * @return The total number of possible entries in the segment.
   */
  public long range() {
    return range;
  }

  /**
   * Returns the maximum entry size for the segment.
   *
   * @return The maximum number of bytes for each entry in the segment.
   */
  public int maxEntrySize() {
    return maxEntrySize;
  }

  /**
   * Returns the maximum size of the segment.
   *
   * @return The maximum allowed size of the segment.
   */
  public int maxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Returns last time the segment was updated.
   * <p>
   * When the segment is first constructed, the {@code updated} time is {@code 0}. Once all entries in the segment have
   * been committed, the {@code updated} time should be set to the current time. Log compaction should not result in a
   * change to {@code updated}.
   *
   * @return The last time the segment was updated in terms of milliseconds since the epoch.
   */
  public long updated() {
    return updated;
  }

  /**
   * Writes an update to the descriptor.
   */
  void update(long timestamp) {
    if (!locked) {
      buffer.writeLong(39, timestamp);
      this.updated = timestamp;
    }
  }

  /**
   * Returns whether the segment has been locked by commitment.
   * <p>
   * Segments will be locked once all entries have been committed to the segment. The lock state of each segment is used
   * to determine log compaction and recovery behavior.
   *
   * @return Indicates whether the segment has been locked.
   */
  public boolean locked() {
    return locked;
  }

  /**
   * Locks the segment.
   */
  void lock() {
    buffer.writeBoolean(47, true).flush();
    locked = true;
  }

  /**
   * Copies the segment to a new buffer.
   */
  SegmentDescriptor copyTo(Buffer buffer) {
    this.buffer = buffer
      .writeLong(id)
      .writeLong(version)
      .writeLong(index)
      .writeLong(range)
      .writeUnsignedMedium(maxEntrySize)
      .writeInt(maxSegmentSize)
      .writeLong(updated)
      .writeBoolean(locked)
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
    if (buffer instanceof FileBuffer)
      ((FileBuffer) buffer).delete();
  }

  /**
   * Segment descriptor builder.
   */
  public static class Builder {
    private final Buffer buffer;

    private Builder(Buffer buffer) {
      if (buffer == null)
        throw new NullPointerException("buffer cannot be null");
      this.buffer = buffer;
    }

    /**
     * Sets the segment identifier.
     *
     * @param id The segment identifier.
     * @return The segment descriptor builder.
     */
    public Builder withId(long id) {
      buffer.writeLong(0, id);
      return this;
    }

    /**
     * Sets the segment version.
     *
     * @param version The segment version.
     * @return The segment descriptor builder.
     */
    public Builder withVersion(long version) {
      buffer.writeLong(8, version);
      return this;
    }

    /**
     * Sets the segment index.
     *
     * @param index The segment starting index.
     * @return The segment descriptor builder.
     */
    public Builder withIndex(long index) {
      buffer.writeLong(16, index);
      return this;
    }

    /**
     * Sets the segment range.
     *
     * @param range The segment range.
     * @return The segment descriptor builder.
     */
    public Builder withRange(long range) {
      buffer.writeLong(24, range);
      return this;
    }

    /**
     * Sets the maximum entry size for the segment.
     *
     * @param maxEntrySize The maximum entry size for the segment.
     * @return The segment descriptor builder.
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      buffer.writeUnsignedMedium(32, maxEntrySize);
      return this;
    }

    /**
     * Sets the number of entries in the segment.
     *
     * @param entries The number of entries in the segment.
     * @return The segment descriptor builder.
     */
    public Builder withMaxSegmentSize(int entries) {
      buffer.writeInt(35, entries);
      return this;
    }

    /**
     * Builds the segment descriptor.
     *
     * @return The built segment descriptor.
     */
    public SegmentDescriptor build() {
      return new SegmentDescriptor(buffer.writeLong(39, 0).rewind());
    }

  }

}
