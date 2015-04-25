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

import java.util.ConcurrentModificationException;

/**
 * Log segment.
 * <p>
 * Each log consists of a set of segments. Each segment represents a unique portion of the log. Because of the semantics
 * of the log compaction algorithm, each segment may contain a fixed but configurable number of entries.
 * <p>
 * Entries written to the segment are indexed in a fixed sized {@link OffsetIndex}. When entries are read from the segment,
 * the offset index is searched for the relative position of the index.
 * <p>
 * Segments are designed to facilitate the immediate effective removal of duplicate entries from within the segment. To
 * achieve this, each segment maintains a light-weight in-memory {@link net.kuujo.copycat.protocol.raft.storage.compact.KeyTable}.
 * When a new entry is committed to the segment, its key is hashed and its offset stored in the lookup table. When an entry
 * is read from the segment, the lookup table is checked to determine whether the entry is the most recent entry for its key
 * in the segment. If a newer entry with the same key has already been written to the lookup table, the segment will behave
 * as if the entry no longer exists.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Segment implements AutoCloseable {
  private static final int ENTRY_TYPE_SIZE = 1;
  private static final int ENTRY_TERM_SIZE = 8;
  private static final int KEY_LENGTH_SIZE = 2;

  /**
   * Opens a new segment.
   *
   * @param buffer The segment buffer.
   * @param descriptor The segment descriptor.
   * @param index The segment index.
   * @return The opened segment.
   */
  public static Segment open(Buffer buffer, SegmentDescriptor descriptor, OffsetIndex index) {
    return new Segment(buffer, descriptor, index);
  }

  private final SegmentDescriptor descriptor;
  private final Buffer source;
  private final Buffer writeBuffer;
  private final Buffer readBuffer;
  private final OffsetIndex offsetIndex;
  private final RaftEntryPool entryPool = new CommittingRaftEntryPool();
  private long commitIndex = 0;
  private long recycleIndex = 0;
  private long skip = 0;
  private boolean open = true;
  private boolean entryLock;

  Segment(Buffer buffer, SegmentDescriptor descriptor, OffsetIndex offsetIndex) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    if (descriptor == null)
      throw new NullPointerException("descriptor cannot be null");
    if (offsetIndex == null)
      throw new NullPointerException("index cannot be null");

    this.source = buffer;
    this.writeBuffer = buffer.slice();
    this.readBuffer = writeBuffer.asReadOnlyBuffer();
    this.descriptor = descriptor;
    this.offsetIndex = offsetIndex;
    if (offsetIndex.size() > 0) {
      writeBuffer.position(offsetIndex.position(offsetIndex.lastOffset()));
    }
  }

  /**
   * Returns the segment descriptor.
   *
   * @return The segment descriptor.
   */
  public SegmentDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns a boolean value indicating whether the segment is open.
   *
   * @return Indicates whether the segment is open.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Returns a boolean value indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return offsetIndex.length() == 0;
  }

  /**
   * Returns a boolean value indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return offset(nextIndex()) >= descriptor.range();
  }

  /**
   * Returns a boolean value indicating whether the segment is immutable.
   *
   * @return Indicates whether the segment is immutable.
   */
  public boolean isLocked() {
    return descriptor.locked();
  }

  /**
   * Returns the total size of the segment in bytes.
   *
   * @return The size of the segment in bytes.
   */
  public long size() {
    return writeBuffer.offset() + writeBuffer.position();
  }

  /**
   * Returns the current range of the segment.
   *
   * @return The current range of the segment.
   */
  public long length() {
    return offsetIndex.lastOffset() + 1;
  }

  /**
   * Returns the number of entries remaining in the segment.
   *
   * @return The number of entries remaining in the segment.
   */
  public int remaining() {
    return !isEmpty() ? descriptor.range() - (offsetIndex.lastOffset() + (int) skip) : descriptor.range() - (int) skip;
  }

  /**
   * Returns the index of the first entry in the segment.
   *
   * @return The index of the first entry in the segment or {@code 0} if the segment is empty.
   */
  public long firstIndex() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() ? descriptor.index() : 0;
  }

  /**
   * Returns the index of the last entry in the segment.
   *
   * @return The index of the last entry in the segment or {@code 0} if the segment is empty.
   */
  public long lastIndex() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() ? offsetIndex.lastOffset() + descriptor.index() : 0;
  }

  /**
   * Returns the next index in the segment.
   *
   * @return The next index in the segment.
   */
  public long nextIndex() {
    return !isEmpty() ? lastIndex() + skip + 1 : descriptor.index() + skip;
  }

  /**
   * Returns the offset for the given index.
   */
  private int offset(long index) {
    return (int) (index - descriptor.index());
  }

  /**
   * Returns the segment's current commit index.
   *
   * @return The segment's current commit index or {@code 0} if no entries have been committed to the segment.
   */
  public long commitIndex() {
    return commitIndex;
  }

  /**
   * Returns the segment's current recycle index.
   *
   * @return The segment's current recycle index or {@code 0} if no entries in the segment have been recycled.
   */
  public long recycleIndex() {
    return recycleIndex;
  }

  /**
   * Checks the range of the given index.
   */
  private void checkRange(long index) {
    if (isEmpty())
      throw new IndexOutOfBoundsException("segment is empty");
    if (index < firstIndex())
      throw new IndexOutOfBoundsException(index + " is less than the first index in the segment");
    if (index > lastIndex())
      throw new IndexOutOfBoundsException(index + " is greater than the last index in the segment");
  }

  /**
   * Returns the absolute entry type position for the entry at the given position.
   */
  private long entryTypePosition(long position) {
    return position;
  }

  /**
   * Returns the absolute entry term position for the entry at the given position.
   */
  private long entryTermPosition(long position) {
    return position + ENTRY_TYPE_SIZE;
  }

  /**
   * Returns the absolute key length position for the entry at the given position.
   */
  private long keyLengthPosition(long position) {
    return position + ENTRY_TYPE_SIZE + ENTRY_TERM_SIZE;
  }

  /**
   * Returns the absolute key position for the entry at the given position.
   */
  private long keyPosition(long position) {
    return position + ENTRY_TYPE_SIZE + ENTRY_TERM_SIZE + KEY_LENGTH_SIZE;
  }

  /**
   * Returns the absolute entry position for the entry at the given position.
   */
  private long entryPosition(long position, int keyLength) {
    return position + ENTRY_TYPE_SIZE + ENTRY_TERM_SIZE + KEY_LENGTH_SIZE + Math.max(keyLength, 0);
  }

  /**
   * Creates a new entry.
   *
   * @return The created entry.
   */
  public RaftEntry createEntry() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    if (isLocked())
      throw new IllegalStateException("segment is locked");
    if (entryLock)
      throw new IllegalStateException("entry outstanding");
    RaftEntry entry = entryPool.acquire(nextIndex());
    entryLock = true;
    return entry;
  }

  /**
   * Writes an absolute entry to the segment.
   */
  public long transferEntry(RaftEntry entry) {
    // Record the starting position of the new entry.
    long position = writeBuffer.position();

    // Write the entry type identifier and 16-bit signed key size.
    writeBuffer.limit(-1).writeByte(entry.readType().id()).writeLong(entry.readTerm());

    long keyLengthPosition = writeBuffer.position();
    writeBuffer.skip(2);

    entry.readKey(writeBuffer.limit(writeBuffer.position() + Math.min(writeBuffer.maxCapacity() - writeBuffer.position(), descriptor.maxKeySize())));

    short keyLength = (short) (writeBuffer.position() - (keyLengthPosition + 2));
    writeBuffer.writeShort(keyLengthPosition, keyLength);

    entry.readEntry(writeBuffer.limit(writeBuffer.position() + Math.min(writeBuffer.maxCapacity() - writeBuffer.position(), descriptor.maxEntrySize())));

    int totalLength = (int) (writeBuffer.position() - position);
    offsetIndex.index(offset(entry.index()), position, totalLength);

    entryLock = false;

    return entry.index();
  }

  /**
   * Commits an entry to the segment.
   */
  void commitEntry(RaftEntry entry) {
    long nextIndex = nextIndex();
    if (entry.index() < nextIndex) {
      throw new CommitModificationException("cannot modify committed entry");
    }
    if (entry.index() > nextIndex) {
      throw new ConcurrentModificationException("attempt to commit entry with non-monotonic index");
    }
    transferEntry(entry.asReadOnlyEntry());
  }

  /**
   * Reads the entry at the given index.
   *
   * @param index The index from which to read the entry.
   * @return The entry at the given index.
   */
  public RaftEntry getEntry(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    checkRange(index);

    // Get the offset of the index within this segment.
    int offset = offset(index);

    // Get the start position of the offset from the offset index.
    long position = offsetIndex.position(offset);

    // If the position is -1 then that indicates no start position was found. The offset may have been removed from
    // the index via deduplication or compaction.
    if (position != -1) {

      // Get the length of the offset entry from the offset index. This will be calculated by getting the start
      // position of the next offset in the index and subtracting this position from the next position.
      int length = offsetIndex.length(offset);

      // Acquire an entry from the entry pool. This will create an entry if all entries in the pool are currently referenced.
      RaftEntry entry = entryPool.acquire(index);
      entry.writeType(RaftEntry.Type.forId(readBuffer.readByte(entryTypePosition(position))));
      entry.writeTerm(readBuffer.readLong(entryTermPosition(position)));

      // Reset the pooled entry with a slice of the underlying buffer using the entry position and length.
      int keySize = readBuffer.readShort(keyLengthPosition(position));
      try (Buffer key = readBuffer.slice(keyPosition(position), keySize)) {
        entry.writeKey(key);
      }

      long entryPosition = entryPosition(position, keySize);
      try (Buffer value = readBuffer.slice(entryPosition, length - (entryPosition - position))) {
        entry.writeEntry(value);
      }
      return entry.asReadOnlyEntry();
    }
    return null;
  }

  /**
   * Returns a boolean value indicating whether the given index is within the range of the segment.
   *
   * @param index The index to check.
   * @return Indicates whether the given index is within the range of the segment.
   */
  public boolean containsIndex(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() && index >= descriptor.index() && index <= lastIndex();
  }

  /**
   * Returns a boolean value indicating whether the entry at the given index is active.
   *
   * @param index The index to check.
   * @return Indicates whether the entry at the given index is active.
   */
  public boolean containsEntry(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return containsIndex(index) && offsetIndex.contains(offset(index));
  }

  /**
   * Skips a number of entries in the segment.
   *
   * @param entries The number of entries to skip.
   * @return The segment.
   */
  public Segment skip(long entries) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    this.skip += entries;
    return this;
  }

  /**
   * Truncates entries after the given index.
   *
   * @param index The index after which to remove entries.
   * @return The segment.
   */
  public Segment truncate(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    if (index <= commitIndex)
      throw new CommitModificationException("cannot truncate entries prior to commit index: " + commitIndex);
    int offset = offset(index);
    if (offset < offsetIndex.lastOffset()) {
      int diff = offsetIndex.lastOffset() - offset;
      skip = Math.max(skip - diff, 0);
      offsetIndex.truncate(offset);
      offsetIndex.flush();
    }
    return this;
  }

  /**
   * Commits entries to the segment.
   *
   * @param index The index up to which to commit entries.
   * @return The segment.
   */
  public Segment commit(long index) {
    checkRange(index);

    if (index > commitIndex) {
      writeBuffer.flush();
      offsetIndex.flush();
      commitIndex = index;
      if (offset(commitIndex) == descriptor.range() - 1) {
        descriptor.update(System.currentTimeMillis());
        descriptor.lock();
      }
    }
    return this;
  }

  /**
   * Recycles entries up to the given index.
   *
   * @param index The index up to which to recycle entries.
   * @return The segment.
   */
  public Segment recycle(long index) {
    recycleIndex = Math.max(recycleIndex, index);
    return this;
  }

  /**
   * Flushes the segment buffers to disk.
   *
   * @return The segment.
   */
  public Segment flush() {
    writeBuffer.flush();
    offsetIndex.flush();
    return this;
  }

  @Override
  public void close() {
    readBuffer.close();
    writeBuffer.close();
    source.close();
    offsetIndex.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    descriptor.delete();
  }

  @Override
  public String toString() {
    return String.format("Segment[id=%d, version=%d, index=%d, length=%d]", descriptor.id(), descriptor.version(), descriptor.index(), length());
  }

  /**
   * Entry pool implementation that commits entries once closed.
   */
  private class CommittingRaftEntryPool extends RaftEntryPool {
    @Override
    public void release(RaftEntry reference) {
      if (!reference.isReadOnly())
        commitEntry(reference);
      super.release(reference);
    }
  }

}
