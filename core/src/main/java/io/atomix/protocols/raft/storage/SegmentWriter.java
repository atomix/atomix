/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage;

import io.atomix.protocols.raft.storage.entry.Entry;
import io.atomix.util.buffer.Buffer;
import io.atomix.util.buffer.HeapBuffer;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static io.atomix.util.buffer.Bytes.BOOLEAN;
import static io.atomix.util.buffer.Bytes.BYTE;
import static io.atomix.util.buffer.Bytes.INTEGER;
import static io.atomix.util.buffer.Bytes.LONG;

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
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentWriter implements Writer {
  private final Segment segment;
  private final Buffer buffer;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private final long firstIndex;
  private Indexed<? extends Entry<?>> lastEntry;

  public SegmentWriter(Segment segment, Buffer buffer) {
    this.segment = segment;
    this.buffer = buffer;
    this.firstIndex = segment.descriptor().index();
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

    // Read the entry length.
    int length = buffer.mark().readInt();

    // If the length is non-zero, read the entry.
    while (length > 0 && (index == 0 || nextIndex <= index)) {

      // Read the entry into memory.
      buffer.read(memory.clear().limit(length));
      memory.flip();

      // Read the checksum of the entry.
      final long checksum = memory.readUnsignedInt();

      // Read the term if defined, otherwise use the term from the previous entry.
      final long term;
      if (memory.readBoolean()) {
        term = memory.readLong();
      } else if (lastEntry != null) {
        term = lastEntry.term();
      } else {
        throw new IllegalStateException("Corrupted log: No entry term found");
      }

      // Read the entry type.
      final int typeId = memory.readByte();

      // Calculate the entry position and length.
      final int entryPosition = (int) memory.position();
      final int entryLength = length - entryPosition;

      // Compute the checksum for the entry bytes.
      final Checksum crc32 = new CRC32();
      crc32.update(memory.array(), entryPosition, entryLength);

      // If the stored checksum equals the computed checksum, return the entry.
      if (checksum == crc32.getValue()) {
        final Entry.Type<?> type = Entry.Type.forId(typeId);
        final Entry entry = type.serializer().readObject(memory, type.type());
        lastEntry = new Indexed<>(nextIndex, term, entry, length);
        nextIndex++;
      } else {
        break;
      }

      // Read the next entry length.
      length = buffer.mark().readInt();
    }

    // Reset the buffer to the previous mark.
    buffer.reset();
  }

  /**
   * Returns the segment to which the writer belongs.
   *
   * @return The segment to which the writer belongs.
   */
  public Segment segment() {
    return segment;
  }

  @Override
  public long lastIndex() {
    return lastEntry != null ? lastEntry.index() : segment.index() - 1;
  }

  @Override
  public Indexed<? extends Entry<?>> lastEntry() {
    return lastEntry;
  }

  @Override
  public long nextIndex() {
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
    return size() >= segment.descriptor().maxSegmentSize()
        || nextIndex() - firstIndex >= segment.descriptor().maxEntries();
  }

  /**
   * Returns the first index written to the segment.
   */
  public long firstIndex() {
    return firstIndex;
  }

  /**
   * Appends an already indexed entry to the segment.
   *
   * @param entry The indexed entry to append.
   * @param <T>   The entry type.
   * @return The updated indexed entry.
   */
  public <T extends Entry<T>> Indexed<T> append(Indexed<T> entry) {
    final long nextIndex = nextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (entry.index() > nextIndex) {
      throw new IndexOutOfBoundsException("Entry index is not sequential");
    }

    // If the entry's index is less than the next index, truncate the segment.
    if (entry.index() < nextIndex) {
      truncate(entry.index() - 1);
    }

    // Append the entry to the segment.
    return append(entry.term(), entry.entry());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> append(long term, T entry) {
    // Clear the in-memory buffer state.
    memory.clear();

    // Store the entry index.
    final long index = nextIndex();

    // Determine whether to skip writing the term to the entry.
    final boolean skipTerm = lastEntry != null && term == lastEntry.term();

    // Calculate the length of the entry header bytes.
    final int headerLength = INTEGER + BOOLEAN + (skipTerm ? 0 : LONG) + BYTE;

    // Clear the memory and skip the header.
    memory.clear().skip(headerLength);

    // Serialize the entry into the in-memory buffer.
    entry.type().serializer().writeObject(memory, entry);

    // Flip the in-memory buffer indexes.
    memory.flip();

    // The total length of the entry is the in-memory buffer limit.
    final int totalLength = (int) memory.limit();

    // Calculate the length of the serialized bytes based on the in-memory buffer limit and header length.
    final int entryLength = totalLength - headerLength;

    // Compute the checksum for the entry.
    final Checksum crc32 = new CRC32();
    crc32.update(memory.array(), headerLength, entryLength);
    final long checksum = crc32.getValue();

    // Rewind the in-memory buffer and write the length, checksum, and offset.
    memory.rewind()
        .writeUnsignedInt(checksum);

    // If the term has not yet been written, write the term to this entry and update the last term.
    if (skipTerm) {
      memory.writeBoolean(false);
    } else {
      memory.writeBoolean(true).writeLong(term);
    }

    // Write the entry type to the memory buffer.
    memory.writeByte(entry.type().id());

    // Write the entry length and entry to the segment.
    buffer.writeInt(totalLength)
        .write(memory.rewind());

    // Return the indexed entry with the correct index/term/length.
    lastEntry = new Indexed<>(index, term, entry, totalLength);
    return (Indexed<T>) lastEntry;
  }

  @Override
  @SuppressWarnings("unchecked")
  public SegmentWriter truncate(long index) {
    // If the index is greater than or equal to the last index, skip the truncate.
    if (index >= lastIndex()) {
      return this;
    }

    // If the index is less than the segment index, clear the segment buffer.
    if (index < segment.index()) {
      buffer.zero().clear();
      return this;
    }

    // Reset the last entry.
    lastEntry = null;

    // Reset the writer to the given index.
    reset(index);

    // Zero entries after the given index.
    buffer.zero(buffer.position());
    return this;
  }

  @Override
  public SegmentWriter flush() {
    buffer.flush();
    return this;
  }

  @Override
  public void close() {
    buffer.close();
  }
}