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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.raft.log.entry.Entry;
import net.kuujo.copycat.raft.log.entry.TypedEntryPool;
import net.kuujo.copycat.util.ExecutionContext;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Raft log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Log implements AutoCloseable {

  /**
   * Returns a new Raft storage builder.
   *
   * @return A new Raft storage builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected final SegmentManager segments;
  protected Compactor compactor;
  protected final TypedEntryPool entryPool = new TypedEntryPool();
  private boolean open;

  protected Log(SegmentManager segments) {
    this.segments = segments;
  }

  /**
   * Opens the log.
   *
   * @param context The context in which to open the log.
   */
  public void open(ExecutionContext context) {
    segments.open(context);
    compactor.open(context);
    open = true;
  }

  /**
   * Returns the log compactor.
   *
   * @return The log compactor.
   */
  public Compactor compactor() {
    return compactor;
  }

  /**
   * Returns the log segment manager.
   *
   * @return The log segment manager.
   */
  SegmentManager segments() {
    return segments;
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
  private void checkOpen() {
    if (!isOpen())
      throw new IllegalStateException("log is not open");
  }

  /**
   * Asserts that the index is a valid index.
   */
  private void checkIndex(long index) {
    if (!containsIndex(index))
      throw new IndexOutOfBoundsException(index + " is not a valid log index");
  }

  /**
   * Returns a boolean value indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public boolean isEmpty() {
    return segments.firstSegment().isEmpty();
  }

  /**
   * Returns the size of the log on disk in bytes.
   *
   * @return The size of the log in bytes.
   */
  public long size() {
    return segments.segments().stream().mapToLong(Segment::size).sum();
  }

  /**
   * Returns the number of entries in the log.
   * <p>
   * The length is the number of physical entries on disk. Note, however, that the length of the log may actually differ
   * from the number of entries eligible for reads due to deduplication.
   *
   * @return The number of entries in the log.
   */
  public long length() {
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
    }
  }

  /**
   * Creates a new log entry.
   * <p>
   * Users should ensure that the returned {@link net.kuujo.copycat.raft.log.entry.Entry} is closed once the write is complete. Closing the entry will
   * result in its contents being persisted to the log. Only a single {@link net.kuujo.copycat.raft.log.entry.Entry} instance may be open via the
   * this method at any given time.
   *
   * @param type The entry type.
   * @return The log entry.
   * @throws IllegalStateException If the log is not open
   * @throws NullPointerException If the entry type is {@code null}
   */
  public <T extends Entry<T>> T createEntry(Class<T> type) {
    checkOpen();
    checkRoll();
    return entryPool.acquire(type, segments.currentSegment().nextIndex());
  }

  /**
   * Appends an entry to the log.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws java.lang.NullPointerException If the entry is {@code null}
   * @throws java.lang.IndexOutOfBoundsException If the entry's index does not match
   *         the expected next log index.
   */
  public long appendEntry(Entry entry) {
    checkOpen();
    checkRoll();
    return segments.currentSegment().appendEntry(entry);
  }

  /**
   * Gets an entry from the log at the given index.
   * <p>
   * If the given index is outside of the bounds of the log then a {@link IndexOutOfBoundsException} will be
   * thrown. If the entry at the given index has been compacted from the then the returned entry will be {@code null}.
   * <p>
   * Entries returned by this method are pooled and {@link net.kuujo.alleycat.io.util.ReferenceCounted}. In order to ensure
   * the entry is released back to the internal entry pool call {@link net.kuujo.copycat.raft.log.entry.Entry#close()} or load the entry in a
   * try-with-resources statement.
   * <pre>
   *   {@code
   *   try (RaftEntry entry = log.getEntry(123)) {
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
  public <T extends Entry<T>> T getEntry(long index) {
    checkOpen();
    checkIndex(index);
    Segment segment = segments.segment(index);
    if (segment == null)
      throw new IndexOutOfBoundsException("invalid index: " + index);
    return segment.getEntry(index);
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
  public boolean containsIndex(long index) {
    return !isEmpty() && firstIndex() <= index && index <= lastIndex();
  }

  /**
   * Returns a boolean value indicating whether the log contains a live entry at the given index.
   *
   * @param index The index to check.
   * @return Indicates whether the log contains a live entry at the given index.
   * @throws IllegalStateException If the log is not open.
   */
  public boolean containsEntry(long index) {
    if (!containsIndex(index))
      return false;
    Segment segment = segments.segment(index);
    return segment != null && segment.containsEntry(index);
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
    checkOpen();
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
    checkOpen();
    checkIndex(index);
    if (lastIndex() == index)
      return this;

    for (Segment segment : segments.segments()) {
      if (segment.containsIndex(index)) {
        segment.truncate(index);
      } else if (segment.descriptor().index() > index) {
        segments.remove(segment);
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
    segments.currentSegment().flush();
  }

  /**
   * Closes the log.
   */
  @Override
  public void close() {
    segments.close();
    compactor.close();
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

  /**
   * Raft log builder.
   */
  public static class Builder implements net.kuujo.copycat.Builder<Log> {
    private final LogConfig config = new LogConfig();

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
     * for each unique log instance.
     *
     * @param directory The log directory.
     * @return The log builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      config.setDirectory(directory);
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
     * for each unique log instance.
     *
     * @param directory The log directory.
     * @return The log builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      config.setDirectory(directory);
      return this;
    }

    /**
     * Sets the log storage level.
     * <p>
     * The storage level dictates how entries in the log are persisted. By default, the {@link StorageLevel#DISK} level
     * is used to persist entries to disk.
     *
     * @param level The storage level.
     * @return The log builder.
     * @throws java.lang.NullPointerException If the {@code level} is {@code null}
     */
    public Builder withStorageLevel(StorageLevel level) {
      config.setStorageLevel(level);
      return this;
    }

    /**
     * Sets the maximum entry size, returning the builder for method chaining.
     * <p>
     * The maximum entry size will be used to place an upper limit on the size of log segments.
     *
     * @param maxEntrySize The maximum entry size.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code maxEntrySize} is not positive
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      config.setMaxEntrySize(maxEntrySize);
      return this;
    }

    /**
     * Sets the maximum segment size, returning the builder for method chaining.
     *
     * @param maxSegmentSize The maximum segment size.
     * @return The log builder.
     * @throws java.lang.IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      config.setMaxSegmentSize(maxSegmentSize);
      return this;
    }

    /**
     * Sets the maximum number of allows entries per segment.
     *
     * @param maxEntriesPerSegment The maximum number of entries allowed per segment.
     * @return The log builder.
     * @throws java.lang.IllegalArgumentException If the {@code maxEntriesPerSegment} is not positive
     */
    public Builder withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      config.setMaxEntriesPerSegment(maxEntriesPerSegment);
      return this;
    }

    /**
     * Sets the minor compaction interval.
     *
     * @param compactionInterval The minor compaction interval in milliseconds.
     * @return The log builder.
     * @throws java.lang.IllegalArgumentException If the compaction interval is not positive
     */
    public Builder withMinorCompactionInterval(long compactionInterval) {
      config.setMinorCompactionInterval(compactionInterval);
      return this;
    }

    /**
     * Sets the minor compaction interval.
     *
     * @param compactionInterval The minor compaction interval.
     * @param unit The interval time unit.
     * @return The log builder.
     * @throws java.lang.IllegalArgumentException If the compaction interval is not positive
     */
    public Builder withMinorCompactionInterval(long compactionInterval, TimeUnit unit) {
      config.setMinorCompactionInterval(compactionInterval, unit);
      return this;
    }

    /**
     * Sets the major compaction interval.
     *
     * @param compactionInterval The major compaction interval in milliseconds.
     * @return The log builder.
     * @throws java.lang.IllegalArgumentException If the compaction interval is not positive
     */
    public Builder withMajorCompactionInterval(long compactionInterval) {
      config.setMajorCompactionInterval(compactionInterval);
      return this;
    }

    /**
     * Sets the major compaction interval.
     *
     * @param compactionInterval The major compaction interval.
     * @param unit The interval time unit.
     * @return The log builder.
     * @throws java.lang.IllegalArgumentException If the compaction interval is not positive
     */
    public Builder withMajorCompactionInterval(long compactionInterval, TimeUnit unit) {
      config.setMajorCompactionInterval(compactionInterval, unit);
      return this;
    }

    /**
     * Builds the log.
     *
     * @return A new buffered log.
     */
    public Log build() {
      Log log = new Log(new SegmentManager(config));
      log.compactor = new Compactor(log)
        .withMinorCompactionInterval(config.getMinorCompactionInterval())
        .withMajorCompactionInterval(config.getMajorCompactionInterval());
      return log;
    }
  }

}
