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

import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.storage.journal.index.Position;
import io.atomix.utils.serializer.Namespace;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

/**
 * Log segment reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MappedJournalSegmentReader<E> implements JournalReader<E> {
  private final ByteBuffer buffer;
  private final int maxEntrySize;
  private final JournalIndex index;
  private final Namespace namespace;
  private final long firstIndex;
  private Indexed<E> currentEntry;
  private Indexed<E> nextEntry;

  MappedJournalSegmentReader(
      ByteBuffer buffer,
      JournalSegment<E> segment,
      int maxEntrySize,
      JournalIndex index,
      Namespace namespace) {
    this.buffer = buffer.slice();
    this.maxEntrySize = maxEntrySize;
    this.index = index;
    this.namespace = namespace;
    this.firstIndex = segment.index();
    reset();
  }

  @Override
  public long getFirstIndex() {
    return firstIndex;
  }

  @Override
  public long getCurrentIndex() {
    return currentEntry != null ? currentEntry.index() : 0;
  }

  @Override
  public Indexed<E> getCurrentEntry() {
    return currentEntry;
  }

  @Override
  public long getNextIndex() {
    return currentEntry != null ? currentEntry.index() + 1 : firstIndex;
  }

  @Override
  public void reset(long index) {
    reset();
    Position position = this.index.lookup(index - 1);
    if (position != null) {
      currentEntry = new Indexed<>(position.index() - 1, null, 0);
      buffer.position(position.position());
      readNext();
    }
    while (getNextIndex() < index && hasNext()) {
      next();
    }
  }

  @Override
  public void reset() {
    buffer.position(JournalSegmentDescriptor.BYTES);
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
  public Indexed<E> next() {
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
    final long index = getNextIndex();

    // Mark the buffer so it can be reset if necessary.
    buffer.mark();

    try {
      // Read the length of the entry.
      final int length = buffer.getInt();

      // If the buffer length is zero then return.
      if (length <= 0 || length > maxEntrySize) {
        buffer.reset();
        nextEntry = null;
        return;
      }

      // Read the checksum of the entry.
      long checksum = buffer.getInt() & 0xFFFFFFFFL;

      // Compute the checksum for the entry bytes.
      final CRC32 crc32 = new CRC32();
      ByteBuffer slice = buffer.slice();
      slice.limit(length);
      crc32.update(slice);

      // If the stored checksum equals the computed checksum, return the entry.
      if (checksum == crc32.getValue()) {
        slice.rewind();
        E entry = namespace.deserialize(slice);
        nextEntry = new Indexed<>(index, entry, length);
        buffer.position(buffer.position() + length);
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
    // Do nothing. The writer is responsible for cleaning the mapped buffer.
  }
}
