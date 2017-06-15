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

import java.nio.BufferUnderflowException;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Log segment reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentReader implements Reader {
  private final Segment segment;
  private final Buffer buffer;
  private final Mode mode;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private final long firstIndex;
  private volatile Indexed<? extends Entry<?>> currentEntry;
  private volatile Indexed<? extends Entry<?>> nextEntry;

  public SegmentReader(Segment segment, Buffer buffer, Mode mode) {
    this.segment = segment;
    this.buffer = buffer;
    this.mode = mode;
    this.firstIndex = segment.descriptor().index();
    readNext();
  }

  @Override
  public Mode mode() {
    return mode;
  }

  /**
   * Returns the first index in the segment.
   *
   * @return The first index in the segment.
   */
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public long currentIndex() {
    return currentEntry != null ? currentEntry.index() : 0;
  }

  @Override
  public Indexed<? extends Entry<?>> currentEntry() {
    return currentEntry;
  }

  @Override
  public long nextIndex() {
    return currentEntry != null ? currentEntry.index() + 1 : firstIndex;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> get(long index) {
    // If the current entry is set, use it to determine whether to reset the reader.
    if (currentEntry != null) {
      // If the index matches the current entry index, return the current entry.
      if (index == currentEntry.index()) {
        return (Indexed<T>) currentEntry;
      }

      // If the index is less than the current entry index, reset the reader.
      if (index < currentEntry.index()) {
        reset();
      }
    }

    // Seek to the given index.
    while (hasNext()) {
      if (nextEntry.index() <= index) {
        next();
      } else {
        break;
      }
    }

    // If the current entry's index matches the given index, return it. Otherwise, return null.
    if (currentEntry != null && index == currentEntry.index()) {
      return (Indexed<T>) currentEntry;
    }
    return null;
  }

  @Override
  public Indexed<? extends Entry<?>> reset(long index) {
    return get(index);
  }

  @Override
  public void reset() {
    buffer.clear();
    currentEntry = null;
    nextEntry = null;
    readNext();
  }

  @Override
  public boolean hasNext() {
    // If the next entry is null, check whether a next entry exists.
    if (nextEntry == null) {
      readNext();
    }
    return nextEntry != null;
  }

  @Override
  public Indexed<? extends Entry<?>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Set the current entry to the next entry.
    currentEntry = nextEntry;

    // Reset the next entry to null.
    nextEntry = null;

    // Read the next entry in the segment.
    readNext();

    // Return the current entry.
    return currentEntry;
  }

  /**
   * Reads the next entry in the segment.
   */
  @SuppressWarnings("unchecked")
  private void readNext() {
    // Compute the index of the next entry in the segment.
    final long index = nextIndex();

    // If the reader is configured to only read commits, stop reading and return once the
    // reader reaches uncommitted entries.
    switch (mode) {
      case COMMITS:
        if (index > segment.manager().commitIndex()) {
          return;
        }
    }

    // Mark the buffer so it can be reset if necessary.
    buffer.mark();

    try {
      // Read the length of the entry.
      final int length = buffer.readInt();

      // If the buffer length is zero then return.
      if (length == 0) {
        buffer.reset();
        nextEntry = null;
        return;
      }

      // Read the entry into memory.
      buffer.read(memory.clear().limit(length));
      memory.flip();

      // Read the checksum of the entry.
      long checksum = memory.readUnsignedInt();

      // Read the term if defined.
      final long term;
      if (memory.readBoolean()) {
        term = memory.readLong();
      } else {
        term = currentEntry.term();
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
        nextEntry = new Indexed<>(index, term, entry, length);
      } else {
        buffer.reset();
        nextEntry = null;
      }
    } catch (BufferUnderflowException e) {
      buffer.reset();
      nextEntry = null;
    }
  }

  @Override
  public void close() {
    memory.close();
    buffer.close();
  }
}