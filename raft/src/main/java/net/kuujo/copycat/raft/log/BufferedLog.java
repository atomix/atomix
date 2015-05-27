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

import net.kuujo.copycat.raft.log.compact.CompactionStrategy;
import net.kuujo.copycat.raft.log.compact.Compactor;
import net.kuujo.copycat.raft.log.entry.RaftEntry;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Efficient, self-compacting commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedLog implements RaftLog {

  /**
   * Returns a new buffered log builder.
   *
   * @return A new buffered log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final LogConfig config;
  protected final Compactor compactor;
  protected SegmentManager segments;
  private boolean open;

  protected BufferedLog(LogConfig config) {
    this.config = config;
    this.compactor = new Compactor(segments)
      .withCompactionStrategy(config.getCompactionStrategy());
  }

  @Override
  public void open() {
    this.segments = new SegmentManager(config);
    compactor.schedule(config.getCompactInterval());
    open = true;
  }

  @Override
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

  @Override
  public boolean isEmpty() {
    return segments.firstSegment().isEmpty();
  }

  @Override
  public long size() {
    return segments.segments().stream().mapToLong(Segment::size).sum();
  }

  @Override
  public long length() {
    return segments.segments().stream().mapToLong(Segment::length).sum();
  }

  @Override
  public long firstIndex() {
    return !isEmpty() ? segments.firstSegment().descriptor().index() : 0;
  }

  @Override
  public long lastIndex() {
    return !isEmpty() ? segments.lastSegment().lastIndex() : 0;
  }

  /**
   * Returns the log's current commit index.
   *
   * @return The log's current commit index.
   */
  public long commitIndex() {
    return segments.commitIndex();
  }

  /**
   * Checks whether we need to roll over to a new segment.
   */
  private void checkRoll() {
    if (segments.currentSegment().isFull()) {
      segments.nextSegment();
    }
  }

  @Override
  public <T extends RaftEntry<T>> T createEntry(Class<T> type) {
    checkOpen();
    checkRoll();
    return segments.currentSegment().createEntry(type);
  }

  @Override
  public long appendEntry(RaftEntry entry) {
    checkOpen();
    checkRoll();
    return segments.currentSegment().appendEntry(entry);
  }

  @Override
  public <T extends RaftEntry<T>> T getEntry(long index) {
    checkOpen();
    checkIndex(index);
    Segment segment = segments.segment(index);
    if (segment == null)
      throw new IndexOutOfBoundsException("invalid index: " + index);
    return segment.getEntry(index);
  }

  @Override
  public boolean containsIndex(long index) {
    return !isEmpty() && firstIndex() <= index && index <= lastIndex();
  }

  @Override
  public boolean containsEntry(long index) {
    if (!containsIndex(index))
      return false;
    Segment segment = segments.segment(index);
    return segment != null && segment.containsEntry(index);
  }

  @Override
  public BufferedLog skip(long entries) {
    checkOpen();
    segments.currentSegment().skip(entries);
    return this;
  }

  @Override
  public BufferedLog truncate(long index) {
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

  @Override
  public void commit(long index) {
    // Immediately compact the segments if this commit results in the commission of all entries in a segment.
    long previousCommitIndex = segments.commitIndex();
    segments.commit(index);
    if (segments.segment(previousCommitIndex) != segments.segment(index))
      compact();
  }

  @Override
  public RaftLog filter(RaftEntryFilter filter) {
    compactor.withEntryFilter(filter);
    return this;
  }

  /**
   * Compacts the log in a background thread.
   */
  public void compact() {
    compactor.execute();
  }

  /**
   * Compacts the log in the current thread.
   */
  void compactNow() {
    compactor.run();
  }

  @Override
  public void flush() {
    segments.currentSegment().flush();
  }

  @Override
  public void close() {
    segments.close();
    compactor.close();
    open = false;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public void delete() {
    segments.delete();
  }

  /**
   * Buffered log builder.
   */
  public static class Builder implements RaftLog.Builder {
    private final LogConfig config = new LogConfig();

    /**
     * Sets the log name, returning the builder for method chaining.
     * <p>
     * The name is a required component of any log and will be used to construct log file names.
     *
     * @param name The log name.
     * @return The log builder.
     * @throws NullPointerException If the {@code name} is {@code null}
     */
    public Builder withName(String name) {
      config.setName(name);
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
     * Sets the log compaction strategy, returning the builder for method chaining.
     * <p>
     * The compaction strategy determined how segments of the log are compacted. Compaction is the process of removing
     * duplicate keys within a single segment or across many segments and ultimately combining multiple segments together.
     * This leads to reduced disk space usage and allows entries to be continuously appended to the log.
     * <p>
     * If the {@code compactionStrategy} is {@code null} then the default
     * {@link net.kuujo.copycat.raft.log.compact.LeveledCompactionStrategy} will be used.
     *
     * @param compactionStrategy The log compaction strategy.
     * @return The log builder.
     */
    public Builder withCompactionStrategy(CompactionStrategy compactionStrategy) {
      config.setCompactionStrategy(compactionStrategy);
      return this;
    }

    /**
     * Sets the log compact interval, returning the builder for method chaining.
     * <p>
     * A background thread will periodically attempt to compact the log at the specified interval using the configured
     * {@link CompactionStrategy}
     *
     * @param interval The log compact interval.
     * @param unit The interval time unit.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code compactInterval} is negative
     */
    public Builder withCompactInterval(long interval, TimeUnit unit) {
      return withCompactInterval(unit.toMillis(interval));
    }

    /**
     * Sets the log compact interval, returning the builder for method chaining.
     * <p>
     * A background thread will periodically attempt to compact the log at the specified interval using the configured
     * {@link CompactionStrategy}
     *
     * @param compactInterval The log compact interval in milliseconds.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code compactInterval} is negative
     */
    public Builder withCompactInterval(long compactInterval) {
      config.setCompactInterval(compactInterval);
      return this;
    }

    /**
     * Builds the log.
     *
     * @return A new buffered log.
     */
    public BufferedLog build() {
      return new BufferedLog(config);
    }
  }

}
