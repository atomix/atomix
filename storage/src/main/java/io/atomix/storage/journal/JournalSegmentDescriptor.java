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
 * limitations under the License.
 */
package io.atomix.storage.journal;

import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores information about a {@link JournalSegment} of the log.
 * <p>
 * The segment descriptor manages metadata related to a single segment of the log. Descriptors are stored within the
 * first {@code 64} bytes of each segment in the following order:
 * <ul>
 * <li>{@code id} (64-bit signed integer) - A unique segment identifier. This is a monotonically increasing number within
 * each log. Segments with in-sequence identifiers should contain in-sequence indexes.</li>
 * <li>{@code index} (64-bit signed integer) - The effective first index of the segment. This indicates the index at which
 * the first entry should be written to the segment. Indexes are monotonically increasing thereafter.</li>
 * <li>{@code version} (64-bit signed integer) - The version of the segment. Versions are monotonically increasing
 * starting at {@code 1}. Versions will only be incremented whenever the segment is rewritten to another memory/disk
 * space, e.g. after log compaction.</li>
 * <li>{@code maxSegmentSize} (32-bit unsigned integer) - The maximum number of bytes allowed in the segment.</li>
 * <li>{@code maxEntries} (32-bit signed integer) - The total number of expected entries in the segment. This is the final
 * number of entries allowed within the segment both before and after compaction. This entry count is used to determine
 * the count of internal indexing and deduplication facilities.</li>
 * <li>{@code updated} (64-bit signed integer) - The last update to the segment in terms of milliseconds since the epoch.
 * When the segment is first constructed, the {@code updated} time is {@code 0}. Once all entries in the segment have
 * been committed, the {@code updated} time should be set to the current time. Log compaction should not result in a
 * change to {@code updated}.</li>
 * <li>{@code locked} (8-bit boolean) - A boolean indicating whether the segment is locked. Segments will be locked once
 * all entries have been committed to the segment. The lock state of each segment is used to determine log compaction
 * and recovery behavior.</li>
 * </ul>
 * The remainder of the 64 segment header bytes are reserved for future metadata.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class JournalSegmentDescriptor {
  public static final int BYTES = 64;

  // Current segment version.
  @VisibleForTesting
  static final int VERSION = 1;

  // The lengths of each field in the header.
  private static final int VERSION_LENGTH = Integer.BYTES;     // 32-bit signed integer
  private static final int ID_LENGTH = Long.BYTES;             // 64-bit signed integer
  private static final int INDEX_LENGTH = Long.BYTES;          // 64-bit signed integer
  private static final int MAX_SIZE_LENGTH = Integer.BYTES;    // 32-bit signed integer
  private static final int MAX_ENTRIES_LENGTH = Integer.BYTES; // 32-bit signed integer
  private static final int UPDATED_LENGTH = Long.BYTES;        // 64-bit signed integer

  // The positions of each field in the header.
  private static final int VERSION_POSITION = 0;                                         // 0
  private static final int ID_POSITION = VERSION_POSITION + VERSION_LENGTH;              // 4
  private static final int INDEX_POSITION = ID_POSITION + ID_LENGTH;                     // 12
  private static final int MAX_SIZE_POSITION = INDEX_POSITION + INDEX_LENGTH;            // 20
  private static final int MAX_ENTRIES_POSITION = MAX_SIZE_POSITION + MAX_SIZE_LENGTH;   // 24
  private static final int UPDATED_POSITION = MAX_ENTRIES_POSITION + MAX_ENTRIES_LENGTH; // 28

  /**
   * Returns a descriptor builder.
   * <p>
   * The descriptor builder will write segment metadata to a {@code 48} byte in-memory buffer.
   *
   * @return The descriptor builder.
   */
  public static Builder builder() {
    return new Builder(ByteBuffer.allocate(BYTES));
  }

  /**
   * Returns a descriptor builder for the given descriptor buffer.
   *
   * @param buffer The descriptor buffer.
   * @return The descriptor builder.
   * @throws NullPointerException if {@code buffer} is null
   */
  public static Builder builder(ByteBuffer buffer) {
    return new Builder(buffer);
  }

  private final ByteBuffer buffer;
  private final int version;
  private final long id;
  private final long index;
  private final int maxSegmentSize;
  private final int maxEntries;
  private volatile long updated;
  private volatile boolean locked;

  /**
   * @throws NullPointerException if {@code buffer} is null
   */
  public JournalSegmentDescriptor(ByteBuffer buffer) {
    this.buffer = buffer;
    this.version = buffer.getInt();
    this.id = buffer.getLong();
    this.index = buffer.getLong();
    this.maxSegmentSize = buffer.getInt();
    this.maxEntries = buffer.getInt();
    this.updated = buffer.getLong();
    this.locked = buffer.get() == 1;
  }

  /**
   * Returns the segment version.
   * <p>
   * Versions are monotonically increasing starting at {@code 1}.
   *
   * @return The segment version.
   */
  public int version() {
    return version;
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
   * Returns the maximum count of the segment.
   *
   * @return The maximum allowed count of the segment.
   */
  public int maxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Returns the maximum number of entries allowed in the segment.
   *
   * @return The maximum number of entries allowed in the segment.
   */
  public int maxEntries() {
    return maxEntries;
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
  public void update(long timestamp) {
    if (!locked) {
      buffer.putLong(UPDATED_POSITION, timestamp);
      this.updated = timestamp;
    }
  }

  /**
   * Copies the segment to a new buffer.
   */
  JournalSegmentDescriptor copyTo(ByteBuffer buffer) {
    buffer.putInt(version);
    buffer.putLong(id);
    buffer.putLong(index);
    buffer.putInt(maxSegmentSize);
    buffer.putInt(maxEntries);
    buffer.putLong(updated);
    buffer.put(locked ? (byte) 1 : (byte) 0);
    return this;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("version", version)
        .add("id", id)
        .add("index", index)
        .add("updated", updated)
        .toString();
  }

  /**
   * Segment descriptor builder.
   */
  public static class Builder {
    private final ByteBuffer buffer;

    private Builder(ByteBuffer buffer) {
      this.buffer = checkNotNull(buffer, "buffer cannot be null");
      buffer.putInt(VERSION_POSITION, VERSION);
    }

    /**
     * Sets the segment identifier.
     *
     * @param id The segment identifier.
     * @return The segment descriptor builder.
     */
    public Builder withId(long id) {
      buffer.putLong(ID_POSITION, id);
      return this;
    }

    /**
     * Sets the segment index.
     *
     * @param index The segment starting index.
     * @return The segment descriptor builder.
     */
    public Builder withIndex(long index) {
      buffer.putLong(INDEX_POSITION, index);
      return this;
    }

    /**
     * Sets maximum count of the segment.
     *
     * @param maxSegmentSize The maximum count of the segment.
     * @return The segment descriptor builder.
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      buffer.putInt(MAX_SIZE_POSITION, maxSegmentSize);
      return this;
    }

    /**
     * Sets the maximum number of entries in the segment.
     *
     * @param maxEntries The maximum number of entries in the segment.
     * @return The segment descriptor builder.
     * @deprecated since 3.0.2
     */
    @Deprecated
    public Builder withMaxEntries(int maxEntries) {
      buffer.putInt(MAX_ENTRIES_POSITION, maxEntries);
      return this;
    }

    /**
     * Builds the segment descriptor.
     *
     * @return The built segment descriptor.
     */
    public JournalSegmentDescriptor build() {
      buffer.rewind();
      return new JournalSegmentDescriptor(buffer);
    }
  }
}
