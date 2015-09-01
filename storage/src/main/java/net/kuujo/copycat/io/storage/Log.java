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

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.concurrent.CopycatThreadFactory;

import java.util.concurrent.Executors;

/**
 * Raft log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Log implements AutoCloseable {
  private final SegmentManager segments;
  private final TypedEntryPool entryPool = new TypedEntryPool();
  private final Cleaner cleaner;
  private boolean open = true;

  /**
   * @throws NullPointerException if {@code storage} is null
   */
  protected Log(Storage storage) {
    this.segments = new SegmentManager(storage);
    this.cleaner = new Cleaner(segments, Executors.newScheduledThreadPool(storage.cleanerThreads(), new CopycatThreadFactory("copycat-log-cleaner-%d")));
  }

  /**
   * Returns the log cleaner.
   *
   * @return The log cleaner.
   */
  public Cleaner cleaner() {
    return cleaner;
  }

  /**
   * Returns the log entry serializer.
   *
   * @return The log entry serializer.
   */
  public Serializer serializer() {
    return segments.serializer();
  }

  /**
   * Returns a boolean value indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Asserts that the log is open.
   */
  private void assertIsOpen() {
    Assert.state(isOpen(), "log is not open");
  }

  /**
   * Asserts that the index is a valid index.
   */
  private void assertValidIndex(long index) {
    Assert.index(validIndex(index), "invalid log index: %s", index);
  }

  /**
   * Returns a boolean value indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public boolean isEmpty() {
    assertIsOpen();
    return segments.firstSegment().isEmpty();
  }

  /**
   * Returns the count of the log on disk in bytes.
   *
   * @return The count of the log in bytes.
   * @throws IllegalStateException If the log is not open.
   */
  public long size() {
    assertIsOpen();
    return segments.segments().stream().mapToLong(Segment::size).sum();
  }

  /**
   * Returns the number of entries in the log.
   * <p>
   * The length is the number of physical entries on disk. Note, however, that the length of the log may actually differ
   * from the number of entries eligible for reads due to deduplication.
   *
   * @return The number of entries in the log.
   * @throws IllegalStateException If the log is not open.
   */
  public long length() {
    assertIsOpen();
    return segments.segments().stream().mapToLong(Segment::length).sum();
  }

  /**
   * Returns the log's current first index.
   * <p>
   * If no entries have been written to the log then the first index will be {@code 0}. If the log contains entries then
   * the first index will be {@code 1}.
   *
   * @return The index of the first entry in the log or {@code 0} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public long firstIndex() {
    return !isEmpty() ? segments.firstSegment().descriptor().index() : 0;
  }

  /**
   * Returns the index of the last entry in the log.
   * <p>
   * If no entries have been written to the log then the last index will be {@code 0}.
   *
   * @return The index of the last entry in the log or {@code 0} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public long lastIndex() {
    return !isEmpty() ? segments.lastSegment().lastIndex() : 0;
  }

  /**
   * Checks whether we need to roll over to a new segment.
   */
  private void checkRoll() {
    if (segments.currentSegment().isFull()) {
      segments.nextSegment();
      cleaner.clean();
    }
  }

  /**
   * Creates a new log entry.
   * <p>
   * Users should ensure that the returned {@link Entry} is closed once the write is complete. Closing the entry will
   * result in its contents being persisted to the log. Only a single {@link Entry} instance may be open via the
   * this method at any given time.
   *
   * @param type The entry type.
   * @return The log entry.
   * @throws IllegalStateException If the log is not open
   * @throws NullPointerException If the {@code type} is {@code null}
   */
  public <T extends Entry<T>> T create(Class<T> type) {
    Assert.notNull(type, "type");
    assertIsOpen();
    checkRoll();
    return entryPool.acquire(type, segments.currentSegment().nextIndex());
  }

  /**
   * Appends an entry to the log.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws IllegalStateException If the log is not open
   * @throws NullPointerException If {@code entry} is {@code null}
   * @throws IndexOutOfBoundsException If the entry's index does not match the expected next log index.
   */
  public long append(Entry entry) {
    Assert.notNull(entry, "entry");
    assertIsOpen();
    checkRoll();
    return segments.currentSegment().append(entry);
  }

  /**
   * Gets an entry from the log at the given index.
   * <p>
   * If the given index is outside of the bounds of the log then a {@link IndexOutOfBoundsException} will be
   * thrown. If the entry at the given index has been compacted from the then the returned entry will be {@code null}.
   * <p>
   * Entries returned by this method are pooled and {@link net.kuujo.copycat.util.ReferenceCounted}. In order to ensure
   * the entry is released back to the internal entry pool call {@link Entry#close()} or load the entry in a
   * try-with-resources statement.
   * <pre>
   *   {@code
   *   try (RaftEntry entry = log.get(123)) {
   *     // Do some stuff...
   *   }
   *   }
   * </pre>
   *
   * @param index The index of the entry to get.
   * @return The entry at the given index or {@code null} if the entry doesn't exist.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  public <T extends Entry> T get(long index) {
    assertIsOpen();
    assertValidIndex(index);

    Segment segment = segments.segment(index);
    if (segment == null)
      throw new IndexOutOfBoundsException("invalid index: " + index);
    T entry = segment.get(index);
    return entry != null ? entry : null;
  }

  /**
   * Returns a boolean value indicating whether the given index is within the bounds of the log.
   * <p>
   * If the index is less than {@code 1} or greater than {@link Log#lastIndex()} then this method will return
   * {@code false}, otherwise {@code true}.
   *
   * @param index The index to check.
   * @return Indicates whether the given index is within the bounds of the log.
   * @throws IllegalStateException If the log is not open.
   */
  private boolean validIndex(long index) {
    long firstIndex = firstIndex();
    long lastIndex = lastIndex();
    return !isEmpty() && firstIndex <= index && index <= lastIndex;
  }

  /**
   * Returns a boolean value indicating whether the log contains a live entry at the given index.
   *
   * @param index The index to check.
   * @return Indicates whether the log contains a live entry at the given index.
   * @throws IllegalStateException If the log is not open.
   */
  public boolean contains(long index) {
    if (!validIndex(index))
      return false;

    Segment segment = segments.segment(index);
    return segment != null && segment.contains(index);
  }

  /**
   * Cleans the entry at the given index.
   *
   * @param index The index of the entry to clean.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  public Log clean(long index) {
    assertIsOpen();
    assertValidIndex(index);

    Segment segment = segments.segment(index);
    if (segment != null)
      segment.clean(index);
    return this;
  }

  /**
   * Cleans the given entry from the log.
   *
   * @param entry The entry to clean.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   * @throws NullPointerException if {@code entry} is null
   * @throws IndexOutOfBoundsException If the {@code entry} index is not within the bounds of the log.
   */
  public Log clean(Entry entry) {
    Assert.notNull(entry, "entry");
    return clean(entry.getIndex());
  }

  /**
   * Skips the given number of entries.
   * <p>
   * This method essentially advances the log's {@link Log#lastIndex()} without writing any entries at the interim
   * indices. Note that calling {@code Loggable#truncate()} after {@code skip()} will result in the skipped entries
   * being partially or completely reverted.
   *
   * @param entries The number of entries to skip.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   * @throws IllegalArgumentException If the number of entries is less than {@code 1}
   * @throws IndexOutOfBoundsException If skipping the given number of entries places the index out of the bounds of the log.
   */
  public Log skip(long entries) {
    assertIsOpen();
    Segment segment = segments.currentSegment();
    while (segment.length() + entries > Integer.MAX_VALUE) {
      int skip = Integer.MAX_VALUE - segment.length();
      segment.skip(skip);
      entries -= skip;
      segment = segments.nextSegment();
    }
    segment.skip(entries);
    return this;
  }

  /**
   * Truncates the log up to the given index.
   *
   * @param index The index at which to truncate the log.
   * @return The updated log.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  public Log truncate(long index) {
    assertIsOpen();
    if (index > 0)
      assertValidIndex(index);

    if (lastIndex() == index)
      return this;

    for (Segment segment : segments.segments()) {
      if (index == 0 || segment.validIndex(index)) {
        segment.truncate(index);
      } else if (segment.descriptor().index() > index) {
        segments.removeSegment(segment);
      }
    }
    return this;
  }

  /**
   * Flushes the log to disk.
   *
   * @throws IllegalStateException If the log is not open.
   */
  public void flush() {
    assertIsOpen();
    segments.currentSegment().flush();
  }

  /**
   * Closes the log.
   * 
   * @throws IllegalStateException If the log is not open.
   */
  @Override
  public void close() {
    assertIsOpen();
    flush();
    segments.close();
    cleaner.close();
    open = false;
  }

  /**
   * Returns a boolean value indicating whether the log is closed.
   *
   * @return Indicates whether the log is closed.
   */
  public boolean isClosed() {
    return !open;
  }

  /**
   * Deletes the log.
   */
  public void delete() {
    segments.delete();
  }

  @Override
  public String toString() {
    return String.format("%s[segments=%s]", getClass().getSimpleName(), segments);
  }

}
