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

import io.atomix.serializer.Serializer;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.Bytes;
import io.atomix.storage.buffer.HeapBuffer;
import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.storage.journal.index.Position;

import java.nio.BufferUnderflowException;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Log segment reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class JournalSegmentReader<E> implements JournalReader<E> {
  private final Buffer buffer;
  private final JournalSegmentCache cache;
  private final JournalIndex index;
  private final Serializer serializer;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private final long firstIndex;
  private Indexed<E> currentEntry;
  private Indexed<E> nextEntry;

  public JournalSegmentReader(JournalSegmentDescriptor descriptor, JournalSegmentCache cache, JournalIndex index, Serializer serializer) {
    this.buffer = descriptor.buffer().slice().duplicate();
    this.cache = cache;
    this.index = index;
    this.serializer = serializer;
    this.firstIndex = descriptor.index();
    readNext();
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

    Indexed cachedEntry = cache.get(index);
    if (cachedEntry != null) {
      this.nextEntry = cachedEntry;
      buffer.skip(cachedEntry.size() + Bytes.INTEGER + Bytes.INTEGER);
      return;
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

      // Read the checksum of the entry.
      long checksum = buffer.readUnsignedInt();

      // Read the entry into memory.
      buffer.read(memory.clear().limit(length));
      memory.flip();

      // Compute the checksum for the entry bytes.
      final Checksum crc32 = new CRC32();
      crc32.update(memory.array(), 0, length);

      // If the stored checksum equals the computed checksum, return the entry.
      if (checksum == crc32.getValue()) {
        E entry = serializer.decode(memory.array());
        nextEntry = new Indexed<>(index, entry, length);
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