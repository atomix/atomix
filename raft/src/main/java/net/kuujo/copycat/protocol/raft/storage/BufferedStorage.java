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
 * Buffered Raft storage.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedStorage implements RaftStorage {

  /**
   * Returns a new buffered storage builder.
   *
   * @return A new buffered storage builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final LogConfig config;

  private BufferedStorage(LogConfig config) {
    this.config = config;
  }

  @Override
  public RaftLog createLog(String name) {
    return new BufferedLog(config.copy().withName(name));
  }

  /**
   * Buffered storage builder.
   */
  public static class Builder implements RaftStorage.Builder {
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
    public RaftStorage build() {
      return new BufferedStorage(config);
    }
  }

}
