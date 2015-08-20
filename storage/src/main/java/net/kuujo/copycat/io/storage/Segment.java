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
import net.kuujo.copycat.io.MappedBuffer;
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
  static Segment open(Buffer buffer, SegmentDescriptor descriptor, OffsetIndex offsetIndex, Serializer serializer) {
    return new Segment(buffer, descriptor, offsetIndex, serializer);
  }

  private final SegmentDescriptor descriptor;
  private final Serializer serializer;
  private final Buffer buffer;
  private final OffsetIndex offsetIndex;
  private int skip = 0;
  private boolean open = true;

  Segment(Buffer buffer, SegmentDescriptor descriptor, OffsetIndex offsetIndex, Serializer serializer) {
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    if (descriptor == null)
      throw new NullPointerException("descriptor cannot be null");
    if (offsetIndex == null)
      throw new NullPointerException("offsetIndex cannot be null");
    if (serializer == null)
      throw new NullPointerException("serializer cannot be null");

    this.serializer = serializer;
    this.buffer = buffer;
    this.descriptor = descriptor;
    this.offsetIndex = offsetIndex;

    // Rebuild the index from the segment data.
    int length = buffer.mark().readUnsignedShort();
    while (length != 0) {
      offsetIndex.index(buffer.readInt(), buffer.position());
      length = buffer.skip(length).mark().readUnsignedShort();
    }
    buffer.reset();
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
    return offsetIndex.size() > 0 ? offsetIndex.lastOffset() - offsetIndex.offset() + 1 + skip == 0 : skip == 0;
  }

  /**
   * Returns a boolean value indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return size() >= descriptor.maxSegmentSize()
      || offsetIndex.size() >= descriptor.maxEntries()
      || offsetIndex.lastOffset() + skip + 1 == Integer.MAX_VALUE;
  }

  /**
   * Returns the total count of the segment in bytes.
   *
   * @return The count of the segment in bytes.
   */
  public long size() {
    return buffer.offset() + buffer.position();
  }

  /**
   * Returns the current range of the segment.
   *
   * @return The current range of the segment.
   */
  public int length() {
    return !isEmpty() ? offsetIndex.lastOffset() - offsetIndex.offset() + 1 + skip : 0;
  }

  /**
   * Returns the count of entries in the segment.
   *
   * @return The count of entries in the segment.
   */
  public int count() {
    return offsetIndex.lastOffset() + 1 - offsetIndex.deletes();
  }

  /**
   * Returns the index of the segment.
   *
   * @return The index of the segment.
   */
  long index() {
    return descriptor.index() + offsetIndex.offset();
  }

  /**
   * Returns the index of the first entry in the segment.
   *
   * @return The index of the first entry in the segment or {@code 0} if the segment is empty.
   */
  public long firstIndex() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() ? descriptor.index() + Math.max(0, offsetIndex.offset()) : 0;
  }

  /**
   * Returns the index of the last entry in the segment.
   *
   * @return The index of the last entry in the segment or {@code 0} if the segment is empty.
   */
  public long lastIndex() {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    return !isEmpty() ? offsetIndex.lastOffset() + descriptor.index() + skip : descriptor.index() - 1;
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
      offsetIndex.resetOffset(offset(firstIndex));
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
  public long append(Entry entry) {
    if (isFull())
      throw new IllegalStateException("segment is full");

    long index = nextIndex();

    if (entry.getIndex() != index) {
      throw new IndexOutOfBoundsException("inconsistent index: " + entry.getIndex());
    }

    // Calculate the offset of the entry.
    int offset = offset(index);

    // Mark the starting position of the record and record the starting position of the new entry.
    long position = buffer.mark().position();

    // Serialize the object into the segment buffer.
    serializer.writeObject(entry, buffer.skip(Short.BYTES + Integer.BYTES).limit(-1));

    // Calculate the length of the serialized bytes based on the resulting buffer position and the starting position.
    int length = (int) (buffer.position() - (position + Short.BYTES + Integer.BYTES));

    // Set the entry size.
    entry.setSize(length);

    // Write the length of the entry for indexing.
    buffer.reset().writeUnsignedShort(length).writeInt(offset).skip(length);

    // Index the offset, position, and length.
    offsetIndex.index(offset, position);

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
  public synchronized <T extends Entry> T get(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    checkRange(index);

    // Get the offset of the index within this segment.
    int offset = offset(index);

    // Return null if the offset has been deleted from the segment.
    if (offsetIndex.deleted(offset)) {
      return null;
    }

    // Get the start position of the entry from the memory index.
    long position = offsetIndex.position(offset);

    // If the index contained the entry, read the entry from the buffer.
    if (position != -1) {

      // Read the length of the entry.
      int length = buffer.readUnsignedShort(position);

      // Verify that the entry at the given offset matches.
      int entryOffset = buffer.readInt(position + Short.BYTES);
      if (entryOffset != offset) {
        throw new IllegalStateException("inconsistent index: " + index);
      }

      // Read the entry buffer and deserialize the entry.
      try (Buffer value = buffer.slice(position + Short.BYTES + Integer.BYTES, length)) {
        T entry = serializer.readObject(value);
        entry.setIndex(index).setSize(length);
        return entry;
      }
    }
    return null;
  }

  /**
   * Returns a boolean value indicating whether the given index is within the range of the segment.
   *
   * @param index The index to check.
   * @return Indicates whether the given index is within the range of the segment.
   */
  boolean validIndex(long index) {
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
  public boolean contains(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");

    if (!validIndex(index))
      return false;

    // Check the memory index first for performance reasons.
    int offset = offset(index);
    return offsetIndex.contains(offset) && !offsetIndex.deleted(offset);
  }

  /**
   * Cleans an entry from the segment.
   *
   * @param index The index of the entry to clean.
   * @return The segment.
   */
  public Segment clean(long index) {
    if (!isOpen())
      throw new IllegalStateException("segment not open");
    offsetIndex.delete(offset(index));
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
    int lastOffset = offsetIndex.lastOffset();

    if (offset < lastOffset) {
      int diff = lastOffset - offset;
      skip = Math.max(skip - diff, 0);

      long position = offsetIndex.truncate(offset);
      buffer.position(position)
        .zero(position)
        .flush();
    }
    return this;
  }

  /**
   * Flushes the segment buffers to disk.
   *
   * @return The segment.
   */
  public Segment flush() {
    buffer.flush();
    offsetIndex.flush();
    return this;
  }

  @Override
  public void close() {
    buffer.close();
    offsetIndex.close();
    descriptor.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    } else if (buffer instanceof MappedBuffer) {
      ((MappedBuffer) buffer).delete();
    }

    offsetIndex.delete();
    descriptor.delete();
  }

  @Override
  public String toString() {
    return String.format("Segment[id=%d, version=%d, index=%d, length=%d]", descriptor.id(), descriptor.version(), firstIndex(), length());
  }

}
