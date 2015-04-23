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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.protocol.raft.storage.compact.CompactionStrategy;
import net.kuujo.copycat.protocol.raft.storage.compact.RetentionPolicy;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Efficient, self-compacting commit log.
 * <p>
 * The log is an on-disk commit log designed with specific optimizations for the Raft consensus algorithm. At a low level,
 * the log consists of a number of {@link Segment segments} backed by separate files. Entries consist of a key-value
 * pair and will always be applied with a monotonically increasing {@code index}. The log makes a best-effort attempt
 * to compact {@link BufferedStorage#commit(long) committed} entries with duplicate keys from the log, retaining only the most recent
 * instance of any given key. However, for performance reasons it cannot guarantee that duplicates do not exist.
 * <p>
 * To create a log, use the {@link BufferedStorage#builder()}
 * <pre>
 *   {@code
 *     Log log = BufferedLog.builder()
 *       .withName("my-log")
 *       .withDirectory(System.getProperty("user.dir"))
 *       .build();
 *   }
 * </pre>
 * All entries are pooled and {@link net.kuujo.copycat.io.util.ReferenceCounted}. When writing or reading an entry, the
 * user must call {@link net.kuujo.copycat.protocol.raft.storage.RaftEntry#close()} once complete or the pool will grow endlessly.
 * Calling {@link net.kuujo.copycat.protocol.raft.storage.RaftEntry#close()} on an entry created via
 * {@link BufferedStorage#createEntry()} will result in the entry being persisted to the log, while calling
 * {@link net.kuujo.copycat.protocol.raft.storage.RaftEntry#close()} on an entry acquired via {@link BufferedStorage#getEntry(long)} will result in the entry being released back
 * to the internal entry pool.
 * <p>
 * The log only allows a single entry to be opened via {@link BufferedStorage#createEntry()} at any given time, and it enforces
 * this restriction by throwing a {@link java.util.ConcurrentModificationException} if more than one entry is written to
 * the log at the same time. Any number of entries may be read via {@link BufferedStorage#getEntry(long)} at any given time, but it's
 * important to note that in multi-threaded environments the underlying log contents could change if entries are being
 * read and written concurrently.
 * <p>
 * In addition to appending entries, the log also allows some of operations that allow entries to be removed from the
 * tail of the log as well. Being designed for the Raft consensus algorithm, the log also has a concept of commitment
 * wherein entries are permanently stored to disk. Entries are committed to the log via the {@link BufferedStorage#commit(long)}
 * method. When an entry or set of entries is committed to the log, they cannot later be removed via {@link BufferedStorage#truncate(long)}.
 * Additionally, prior to commitment entries are guaranteed not to be compacted or otherwise automatically removed from
 * the log. However, after commitment entries are freed for compaction.
 * <p>
 * If log compaction is configured (the default), the log will periodically compact itself in a background thread. The
 * log compaction algorithm is key-based and is designed to favor reducing the number of live entries at the tail of the
 * log. Compaction means that the log makes an effort to only retain the single latest entry with any given key. If, for
 * instance, an entry with the key {@code 1111} is written at index {@code 5} and an entry with the same key {@code 1111}
 * is later written at index {@code 10}, once both entries are committed the log guarantees that the first entry at index
 * {@code 5} will eventually be removed from the log. This pattern is useful for state machines wherein only the most
 * recent command contributes to the state for a given key.
 * <p>
 * Compaction is a two-stage process. The first stage performs immediate "virtual" compaction by maintaining an efficient
 * in-memory lookup table of keys in each segment. The lookup table contains a mapping of key hashes to the highest
 * committed index. When an entry is committed, its key is hashed and its index added to the given segments key table.
 * Thereafter, any entries with the same key <i>within the same segment</i> will be ignored during reads, effectively
 * meaning they're removed from the user's perspective.
 * <p>
 * In-memory deduplication of keys only gives the appearance that entries have been removed from the log. However, in
 * order to preserve disk space, the second stage of the compaction process involves combining and rewriting physical
 * segments on disk. As entries are written to the log and new segments are created, the log will periodically evaluate
 * those segments in a background thread. The background compaction algorithm works by counting the number of keys in
 * a set of adjacent segments to determine whether they can be combined. If two adjacent segments can be combined into
 * the size of a single segment (given the size limitations of the {@link net.kuujo.copycat.protocol.raft.storage.compact.KeyTable}
 * and {@link OffsetIndex}), a background thread will create a new segment and rewrite only active entries from each segment
 * in order. The default {@link net.kuujo.copycat.protocol.raft.storage.compact.CompactionStrategy} prioritizes compaction
 * of recent segments over older segments under the assumption that entries present in less recent segments are less
 * likely to be duplicated.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedStorage implements RaftStorage {

  /**
   * Returns a new buffered log builder.
   *
   * @return A new buffered log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Buffered log builder.
   */
  public static class Builder {
    private final StorageConfig config = new StorageConfig();

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
     * Sets the maximum key size, returning the builder for method chaining.
     * <p>
     * The maximum key size will be used to place an upper limit on the size of log segments. Keys are stored as unsigned
     * 16-bit integers and thus the key size cannot be greater than {@code Short.MAX_VALUE * 2}. By default the
     * {@code maxKeySize} is {@code 1024}.
     *
     * @param maxKeySize The maximum key size.
     * @return The log builder.
     * @throws IllegalArgumentException If the {@code maxKeySize} is not positive or is greater than
     * {@code Short.MAX_VALUE * 2}
     */
    public Builder withMaxKeySize(int maxKeySize) {
      config.setMaxKeySize(maxKeySize);
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
     * Sets the number of entries per segment, returning the builder for method chaining.
     * <p>
     * Because of the semantics of key deduplication and compaction, the log requires that each internal segment be of a
     * fixed number of entries. The number of entries per segment must follow the following formula:
     * {@code entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) < Integer.MAX_VALUE * 2}
     *
     * @param entriesPerSegment The number of entries per segment.
     * @return The log builder.
     * @throws IllegalArgumentException If the number of entries per segment does not adhere to the formula
     *         {@code entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) < Integer.MAX_VALUE * 2}
     */
    public Builder withEntriesPerSegment(int entriesPerSegment) {
      config.setEntriesPerSegment(entriesPerSegment);
      return this;
    }

    /**
     * Sets the log retention policy, returning the builder for method chaining.
     * <p>
     * The retention policy dictates the amount of time for which a log segment should be retained. Each time the log
     * is compacted, the compaction strategy will be queried to determine whether any segments should be deleted from the
     * log. Retention policies can base their decision on time, size, or other factors.
     *
     * @param retentionPolicy The log retention policy.
     * @return The log builder.
     * @throws NullPointerException If the {@code retentionPolicy} is {@code null}
     */
    public Builder withRetentionPolicy(RetentionPolicy retentionPolicy) {
      config.setRetentionPolicy(retentionPolicy);
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
     * {@link net.kuujo.copycat.protocol.raft.storage.compact.LeveledCompactionStrategy} will be used.
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
    public BufferedStorage build() {
      return new BufferedStorage(config);
    }
  }

  private final StorageConfig config;
  protected SegmentManager segments;
  private boolean open;

  protected BufferedStorage(StorageConfig config) {
    this.config = config;
  }

  @Override
  public void open() {
    this.segments = new SegmentManager(config);
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

  @Override
  public long nextIndex() {
    checkRoll();
    return segments.lastSegment().nextIndex();
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
   * Returns the log's current recycle index.
   *
   * @return The log's current recycle index.
   */
  public long recycleIndex() {
    return segments.recycleIndex();
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
  public RaftEntry createEntry() {
    checkOpen();
    checkRoll();
    return segments.currentSegment().createEntry();
  }

  @Override
  public RaftEntry getEntry(long index) {
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
  public BufferedStorage skip(long entries) {
    checkOpen();
    long remaining = entries;
    Segment segment = segments.currentSegment();
    while (remaining > 0) {
      long segmentEntries = Math.min(remaining, segment.remaining());
      remaining = remaining - segmentEntries;
      segment.skip(segmentEntries);
      checkRoll();
    }
    return this;
  }

  @Override
  public BufferedStorage truncate(long index) {
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
    segments.commit(index);
  }

  @Override
  public void recycle(long index) {
    segments.recycle(index);
  }

  /**
   * Compacts the log.
   * <p>
   * This method will force compaction to take place <em>in the current thread</em>. Therefore, it may block the calling
   * thread for some period of time while compaction takes place.
   *
   */
  public void compact() {
    segments.compact();
  }

  @Override
  public void flush() {
    segments.currentSegment().flush();
  }

  @Override
  public void close() {
    segments.close();
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

}
