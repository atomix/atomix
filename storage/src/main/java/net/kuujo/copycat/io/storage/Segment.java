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
package net.kuujo.copycat.io.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.FileBuffer;
import net.kuujo.copycat.io.serializer.Serializer;

/**
 * Log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class Segment implements AutoCloseable {

  /**
   * Opens a new segment.
   */
  static Segment open(Buffer diskBuffer, Buffer memoryBuffer, SegmentDescriptor descriptor, OffsetIndex diskIndex, OffsetIndex memoryIndex, Serializer serializer) {
    return new Segment(diskBuffer, memoryBuffer, descriptor, diskIndex, memoryIndex, serializer);
  }

  private final SegmentDescriptor descriptor;
  private final Serializer serializer;
  private final Buffer diskBuffer;
  private final Buffer memoryBuffer;
  private final OffsetIndex diskIndex;
  private final OffsetIndex memoryIndex;
  private int skip = 0;
  private boolean open = true;

  Segment(Buffer diskBuffer, Buffer memoryBuffer, SegmentDescriptor descriptor, OffsetIndex diskIndex, OffsetIndex memoryIndex, Serializer serializer) {
    if (diskBuffer == null)
      throw new NullPointerException("diskBuffer cannot be null");
    if (memoryBuffer == null)
      throw new NullPointerException("memoryBuffer cannot be null");
    if (descriptor == null)
      throw new NullPointerException("descriptor cannot be null");
    if (diskIndex == null)
      throw new NullPointerException("diskIndex cannot be null");
    if (memoryIndex == null)
      throw new NullPointerException("memoryIndex cannot be null");
    if (serializer == null)
      throw new NullPointerException("serializer cannot be null");

    this.serializer = serializer;
    this.diskBuffer = diskBuffer;
    this.memoryBuffer = memoryBuffer;
    this.descriptor = descriptor;
    this.diskIndex = diskIndex;
    this.memoryIndex = memoryIndex;
    if (diskIndex.size() > 0) {
      diskBuffer.position(diskIndex.position(diskIndex.lastOffset()) + diskIndex.length(diskIndex.lastOffset()));
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
    return isEmpty(memoryIndex) && isEmpty(diskIndex);
  }

  /**
   * Returns a boolean value indicating whether the given index is empty.
   */
  private boolean isEmpty(OffsetIndex index) {
    return index.size() > 0 ? index.lastOffset() - index.offset() + 1 + skip == 0 : skip == 0;
  }

  /**
   * Returns a boolean value indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return size() >= descriptor.maxSegmentSize()
      || memoryIndex.size() + diskIndex.size() >= descriptor.maxEntries()
      || Math.max(memoryIndex.lastOffset(), diskIndex.lastOffset()) + skip + 1 == Integer.MAX_VALUE;
  }

  /**
   * Returns the total count of the segment in bytes.
   *
   * @return The count of the segment in bytes.
   */
  public long size() {
    return diskBuffer.offset() + diskBuffer.position() + memoryBuffer.offset() + memoryBuffer.position();
  }

  /**
   * Returns the current range of the segment.
   *
   * @return The current range of the segment.
   */
  public int length() {
    return !isEmpty() ? Math.max(memoryIndex.lastOffset(), diskIndex.lastOffset()) - Math.min(memoryIndex.offset(), diskIndex.offset()) + 1 + skip : 0;
  }

  /**
   * Returns the count of entries in the segment.
   *
   * @return The count of entries in the segment.
   */
  public int count() {
    return Math.max(memoryIndex.lastOffset(), diskIndex.lastOffset()) + 1 - (memoryIndex.deletes() + diskIndex.deletes());
  }

  /**
   * Returns the index of the segment.
   *
   * @return The index of the segment.
   */
  long index() {
    return descriptor.index() + Math.min(memoryIndex.offset(), diskIndex.offset());
  }

  /**
   * Returns the index of the first entry in the segment.
   *
   * @return The index of the first entry in the segment or {@code 0} if the segment is empty.
   */
  public long firstIndex() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() ? descriptor.index() + Math.max(0, Math.min(diskIndex.offset(), memoryIndex.offset())) : 0;
  }

  /**
   * Returns the index of the last entry in the segment.
   *
   * @return The index of the last entry in the segment or {@code 0} if the segment is empty.
   */
  public long lastIndex() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() ? Math.max(diskIndex.lastOffset(), memoryIndex.lastOffset()) + descriptor.index() + skip : descriptor.index() - 1;
  }

  /**
   * Returns the next index in the segment.
   *
   * @return The next index in the segment.
   */
  public long nextIndex() {
    return !isEmpty() ? lastIndex() + 1 : descriptor.index() + skip;
  }

  /**
   * Compacts the head of the segment up to the given index.
   *
   * @param firstIndex The first index in the segment.
   * @return The segment.
   */
  public Segment compact(long firstIndex) {
    if (!isEmpty()) {
      int offset = offset(firstIndex);
      diskIndex.resetOffset(offset);
      memoryIndex.resetOffset(offset);
    }
    return this;
  }

  /**
   * Returns the offset for the given index.
   */
  private int offset(long index) {
    return (int) (index - descriptor.index());
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
   * Commits an entry to the segment.
   */
  public long appendEntry(Entry entry) {
    if (isFull())
      throw new IllegalStateException("segment is full");

    if (entry.getPersistenceLevel() == PersistenceLevel.DISK) {
      return appendEntry(entry, diskBuffer, diskIndex);
    } else {
      return appendEntry(entry, memoryBuffer, memoryIndex);
    }
  }

  /**
   * Appends an entry to the segment.
   */
  private long appendEntry(Entry entry, Buffer buffer, OffsetIndex offsetIndex) {
    long index = nextIndex();

    if (entry.getIndex() != index) {
      throw new IndexOutOfBoundsException("inconsistent index: " + entry.getIndex());
    }

    // Calculate the offset of the entry.
    int offset = offset(index);

    // Record the starting position of the new entry.
    long position = buffer.position();

    // Serialize the object into the segment buffer.
    serializer.writeObject(entry, buffer.limit(-1));

    // Calculate the length of the serialized bytes based on the resulting buffer position and the starting position.
    int length = (int) (buffer.position() - position);

    // Index the offset, position, and length.
    offsetIndex.index(offset, position, length);

    // Reset skip to zero since we wrote a new entry.
    skip = 0;

    return index;
  }

  /**
   * Reads the entry at the given index.
   *
   * @param index The index from which to read the entry.
   * @return The entry at the given index.
   */
  public synchronized <T extends Entry> T getEntry(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    checkRange(index);

    // Get the offset of the index within this segment.
    int offset = offset(index);

    // Return null if the offset has been deleted from the segment.
    if (memoryIndex.deleted(offset) && diskIndex.deleted(offset)) {
      return null;
    }

    // Get the start position of the entry from the memory index.
    long position = memoryIndex.position(offset);

    // If the memory index contained the entry, read the entry from the memory buffer.
    if (position != -1) {
      return getEntry(index, memoryBuffer, position, memoryIndex.length(offset), PersistenceLevel.MEMORY);
    } else {
      // If the memory index did not contain the entry, attempt to read it from disk.
      position = diskIndex.position(offset);
      if (position != -1) {
        return getEntry(index, diskBuffer, position, diskIndex.length(offset), PersistenceLevel.DISK);
      }
    }
    return null;
  }

  /**
   * Reads an entry from the segment.
   */
  private synchronized <T extends Entry> T getEntry(long index, Buffer buffer, long position, int length, PersistenceLevel persistenceLevel) {
    try (Buffer value = buffer.slice(position, length)) {
      T entry = serializer.readObject(value);
      entry.setIndex(index).setPersistenceLevel(persistenceLevel);
      return entry;
    }
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
    return !isEmpty() && index >= firstIndex() && index <= lastIndex();
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

    if (!containsIndex(index)) {
      return false;
    }

    // Check the memory index first for performance reasons.
    int offset = offset(index);
    return (memoryIndex.contains(offset) && !memoryIndex.deleted(offset))
      || (diskIndex.contains(offset) && !diskIndex.deleted(offset));
  }

  /**
   * Cleans an entry from the segment.
   *
   * @param index The index of the entry to clean.
   * @return The segment.
   */
  public Segment cleanEntry(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    int offset = offset(index);
    diskIndex.delete(offset);
    memoryIndex.delete(offset);
    return this;
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

    int offset = offset(index);
    int lastOffset = Math.max(diskIndex.lastOffset(), memoryIndex.lastOffset());

    if (offset < lastOffset) {
      int diff = lastOffset - offset;
      skip = Math.max(skip - diff, 0);
      diskIndex.truncate(offset);
      memoryIndex.truncate(offset);
      diskIndex.flush();
    }
    return this;
  }

  /**
   * Flushes the segment buffers to disk.
   *
   * @return The segment.
   */
  public Segment flush() {
    diskBuffer.flush();
    diskIndex.flush();
    return this;
  }

  @Override
  public void close() {
    diskBuffer.close();
    diskIndex.close();
    memoryBuffer.close();
    memoryIndex.close();
    descriptor.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    if (diskBuffer instanceof FileBuffer) {
      ((FileBuffer) diskBuffer).delete();
    }
    diskIndex.delete();
    descriptor.delete();
  }

  @Override
  public String toString() {
    return String.format("Segment[id=%d, version=%d, index=%d, length=%d]", descriptor.id(), descriptor.version(), firstIndex(), length());
  }

}
