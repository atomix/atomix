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
import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.storage.journal.index.SparseJournalIndex;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

/**
 * Log segment.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class JournalSegment<E> implements AutoCloseable {
  private static final int ENTRY_CACHE_SIZE = 1024;

  protected final JournalSegmentFile file;
  protected final JournalSegmentDescriptor descriptor;
  protected final JournalIndex index;
  protected final Serializer serializer;
  private final JournalSegmentWriter<E> writer;
  private final JournalSegmentCache cache;
  private boolean open = true;

  public JournalSegment(JournalSegmentFile file, JournalSegmentDescriptor descriptor, double indexDensity, int cacheSize, Serializer serializer) {
    this.file = file;
    this.descriptor = descriptor;
    this.index = new SparseJournalIndex(indexDensity);
    this.serializer = serializer;
    this.cache = new JournalSegmentCache(descriptor.index(), cacheSize);
    this.writer = new JournalSegmentWriter<>(descriptor, cache, index, serializer);
  }

  /**
   * Returns the segment ID.
   *
   * @return The segment ID.
   */
  public long id() {
    return descriptor.id();
  }

  /**
   * Returns the segment version.
   *
   * @return The segment version.
   */
  public long version() {
    return descriptor.version();
  }

  /**
   * Returns the segment's starting index.
   *
   * @return The segment's starting index.
   */
  public long index() {
    return descriptor.index();
  }

  /**
   * Returns the last index in the segment.
   *
   * @return The last index in the segment.
   */
  public long lastIndex() {
    return writer.getLastIndex();
  }

  /**
   * Returns the segment file.
   *
   * @return The segment file.
   */
  public JournalSegmentFile file() {
    return file;
  }

  /**
   * Returns the segment descriptor.
   *
   * @return The segment descriptor.
   */
  public JournalSegmentDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns the segment size.
   *
   * @return The segment size.
   */
  public long size() {
    return writer.size();
  }

  /**
   * Returns a boolean value indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return length() == 0;
  }

  /**
   * Returns a boolean indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return writer.isFull();
  }

  /**
   * Returns the segment length.
   *
   * @return The segment length.
   */
  public long length() {
    return writer.getNextIndex() - index();
  }

  /**
   * Returns the segment writer.
   *
   * @return The segment writer.
   */
  public JournalSegmentWriter<E> writer() {
    checkOpen();
    return writer;
  }

  /**
   * Creates a new segment reader.
   *
   * @return A new segment reader.
   */
  JournalSegmentReader<E> createReader() {
    checkOpen();
    return new JournalSegmentReader<>(descriptor, cache, index, serializer);
  }

  /**
   * Checks whether the segment is open.
   */
  private void checkOpen() {
    checkState(open, "Segment not open");
  }

  /**
   * Returns a boolean indicating whether the segment is open.
   *
   * @return indicates whether the segment is open
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Closes the segment.
   */
  @Override
  public void close() {
    writer.close();
    descriptor.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    writer.delete();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id())
        .add("version", version())
        .add("index", index())
        .add("size", size())
        .toString();
  }
}