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
package io.atomix.storage.journal;

import io.atomix.storage.StorageException;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.Bytes;
import io.atomix.storage.buffer.FileBuffer;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.storage.buffer.MappedBuffer;
import io.atomix.storage.buffer.SlicedBuffer;
import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.utils.serializer.Serializer;

import java.nio.BufferOverflowException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Segment writer.
 * <p>
 * The format of an entry in the log is as follows:
 * <ul>
 * <li>64-bit index</li>
 * <li>8-bit boolean indicating whether a term change is contained in the entry</li>
 * <li>64-bit optional term</li>
 * <li>32-bit signed entry length, including the entry type ID</li>
 * <li>8-bit signed entry type ID</li>
 * <li>n-bit entry bytes</li>
 * </ul>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JournalSegmentWriter<E> implements JournalWriter<E> {
  private final JournalSegmentDescriptor descriptor;
  private final int maxEntrySize;
  private final JournalSegmentCache cache;
  private final JournalIndex index;
  private final Buffer buffer;
  private final Serializer serializer;
  private final Buffer memory = HeapBuffer.allocate().flip();
  private final long firstIndex;
  private Indexed<E> lastEntry;

  public JournalSegmentWriter(
      JournalSegmentDescriptor descriptor,
      int maxEntrySize,
      JournalSegmentCache cache,
      JournalIndex index,
      Serializer serializer) {
    this.descriptor = descriptor;
    this.maxEntrySize = maxEntrySize;
    this.cache = cache;
    this.index = index;
    this.buffer = descriptor.buffer().slice();
    this.serializer = serializer;
    this.firstIndex = descriptor.index();
    reset(0);
  }

  /**
   * Initializes the writer by seeking to the end of the segment.
   */
  @SuppressWarnings("unchecked")
  private void reset(long index) {
    long nextIndex = firstIndex;

    // Clear the buffer indexes.
    buffer.clear();
    memory.clear().flip();

    // Record the current buffer position.
    int position = buffer.position();

    // Read more bytes from the segment if necessary.
    if (memory.remaining() < maxEntrySize) {
      buffer.mark()
          .read(memory.clear().limit(maxEntrySize * 2))
          .reset();
      memory.flip();
    }

    // Read the entry length.
    int length = memory.mark().readInt();

    // If the length is non-zero, read the entry.
    while (0 < length && length <= maxEntrySize && (index == 0 || nextIndex <= index)) {

      // Read the checksum of the entry.
      final long checksum = memory.readUnsignedInt();

      // Read the entry into memory.
      byte[] bytes = new byte[length];
      memory.read(bytes);

      // Compute the checksum for the entry bytes.
      final Checksum crc32 = new CRC32();
      crc32.update(bytes, 0, length);

      // If the stored checksum equals the computed checksum, return the entry.
      if (checksum == crc32.getValue()) {
        final E entry = serializer.decode(bytes);
        lastEntry = new Indexed<>(nextIndex, entry, length);
        this.index.index(nextIndex, position);
        nextIndex++;
      } else {
        break;
      }

      // Update the current position for indexing.
      position = buffer.position() + memory.position();

      // Read more bytes from the segment if necessary.
      if (memory.remaining() < maxEntrySize) {
        buffer.skip(memory.position())
            .mark()
            .read(memory.clear().limit(maxEntrySize * 2))
            .reset();
        memory.flip();
      }

      length = memory.mark().readInt();
    }

    // Reset the buffer to the previous mark.
    buffer.skip(memory.reset().position());
  }

  @Override
  public long getLastIndex() {
    return lastEntry != null ? lastEntry.index() : descriptor.index() - 1;
  }

  @Override
  public Indexed<E> getLastEntry() {
    return lastEntry;
  }

  @Override
  public long getNextIndex() {
    if (lastEntry != null) {
      return lastEntry.index() + 1;
    } else {
      return firstIndex;
    }
  }

  /**
   * Returns the size of the underlying buffer.
   *
   * @return The size of the underlying buffer.
   */
  public long size() {
    return buffer.offset() + buffer.position();
  }

  /**
   * Returns a boolean indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return lastEntry == null;
  }

  /**
   * Returns a boolean indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return size() >= descriptor.maxSegmentSize()
        || getNextIndex() - firstIndex >= descriptor.maxEntries();
  }

  /**
   * Returns the first index written to the segment.
   */
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void append(Indexed<E> entry) {
    final long nextIndex = getNextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (entry.index() > nextIndex) {
      throw new IndexOutOfBoundsException("Entry index is not sequential");
    }

    // If the entry's index is less than the next index, truncate the segment.
    if (entry.index() < nextIndex) {
      truncate(entry.index() - 1);
    }
    append(entry.entry());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends E> Indexed<T> append(T entry) {
    // Store the entry index.
    final long index = getNextIndex();

    // Serialize the entry.
    final byte[] bytes = serializer.encode(entry);
    final int length = bytes.length;

    // Ensure there's enough space left in the buffer to store the entry.
    if (buffer.remaining() < length + Bytes.INTEGER + Bytes.INTEGER) {
      throw new BufferOverflowException();
    }

    // If the entry length exceeds the maximum entry size then throw an exception.
    if (length > maxEntrySize) {
      throw new StorageException.TooLarge("Entry size " + length + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
    }

    // Compute the checksum for the entry.
    final Checksum crc32 = new CRC32();
    crc32.update(bytes, 0, length);
    final long checksum = crc32.getValue();

    // Record the current buffer position;
    int position = buffer.position();

    // Create a single byte[] in memory for the entire entry and write it as a batch to the underlying buffer.
    buffer.write(memory.clear()
        .writeInt(length)
        .writeUnsignedInt(checksum)
        .write(bytes)
        .flip());

    // Update the last entry with the correct index/term/length.
    Indexed<E> indexedEntry = new Indexed<>(index, entry, length);
    this.lastEntry = indexedEntry;
    this.cache.put(indexedEntry);
    this.index.index(index, position);
    return (Indexed<T>) indexedEntry;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void truncate(long index) {
    // If the index is greater than or equal to the last index, skip the truncate.
    if (index >= getLastIndex()) {
      return;
    }

    // Reset the last entry.
    lastEntry = null;

    // If the index is less than the segment index, clear the segment buffer.
    if (index < descriptor.index()) {
      buffer.zero().clear();
      this.cache.truncate(index);
      this.index.truncate(index);
      return;
    }

    // Truncate the index.
    this.cache.truncate(index);
    this.index.truncate(index);

    // Reset the writer to the given index.
    reset(index);

    // Zero entries after the given index.
    buffer.zero(buffer.position());
  }

  @Override
  public void flush() {
    buffer.flush();
  }

  @Override
  public void close() {
    buffer.close();
  }

  /**
   * Deletes the segment.
   */
  void delete() {
    Buffer buffer = this.buffer instanceof SlicedBuffer ? ((SlicedBuffer) this.buffer).root() : this.buffer;
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    } else if (buffer instanceof MappedBuffer) {
      ((MappedBuffer) buffer).delete();
    }
  }
}