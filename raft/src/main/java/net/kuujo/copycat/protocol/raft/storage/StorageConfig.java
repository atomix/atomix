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
import net.kuujo.copycat.protocol.raft.storage.compact.FullRetentionPolicy;
import net.kuujo.copycat.protocol.raft.storage.compact.LeveledCompactionStrategy;
import net.kuujo.copycat.protocol.raft.storage.compact.RetentionPolicy;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Copycat storage configuration.
 * <p>
 * The storage configuration is used to create log files, allocate disk space and predict disk usage for each segment in
 * the log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StorageConfig {
  private static final String DEFAULT_NAME = "log";
  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_KEY_SIZE = 1024;
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 8;
  private static final int DEFAULT_ENTRIES_PER_SEGMENT = 1024 * 1024;
  private static final RetentionPolicy DEFAULT_RETENTION_POLICY = new FullRetentionPolicy();
  private static final CompactionStrategy DEFAULT_COMPACTION_STRATEGY = new LeveledCompactionStrategy();
  private static final long DEFAULT_COMPACT_INTERVAL = 1000 * 60;

  // The maximum allowed size of a key.
  private static final int MAX_KEY_SIZE = Short.MAX_VALUE;

  // The maximum number of entries per segment is dictated by the number of entries that can fit into a memory mapped index.
  private static final int MAX_ENTRIES_PER_SEGMENT = Integer.MAX_VALUE / 8;

  // The maximum number of bites allowed in a segment.
  private static final long MAX_SEGMENT_SIZE = (long) Integer.MAX_VALUE * 2;

  private String name = DEFAULT_NAME;
  private File directory = new File(DEFAULT_DIRECTORY);
  private int maxKeySize = DEFAULT_MAX_KEY_SIZE;
  private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  private int entriesPerSegment = DEFAULT_ENTRIES_PER_SEGMENT;
  private RetentionPolicy retentionPolicy = DEFAULT_RETENTION_POLICY;
  private CompactionStrategy compactionStrategy = DEFAULT_COMPACTION_STRATEGY;
  private long compactInterval = DEFAULT_COMPACT_INTERVAL;

  /**
   * Sets the log name.
   * <p>
   * The name is a required component of any log configuration and will be used to construct log file names.
   *
   * @param name The log name.
   * @throws NullPointerException If the {@code name} is {@code null}
   */
  public void setName(String name) {
    if (name == null)
      throw new NullPointerException("name cannot be null");
    this.name = name;
  }

  /**
   * Returns the log name.
   *
   * @return The log name. Defaults to {@code log}
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the log name, returning the configuration for method chaining.
   * <p>
   * The name is a required component of any log configuration and will be used to construct log file names.
   *
   * @param name The log name.
   * @return The log configuration.
   * @throws NullPointerException If the {@code name} is {@code null}
   */
  public StorageConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the log directory.
   * <p>
   * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
   * for each unique log instance.
   *
   * @param directory The log directory.
   * @throws NullPointerException If the {@code directory} is {@code null}
   */
  public void setDirectory(String directory) {
    setDirectory(new File(directory));
  }

  /**
   * Sets the log directory.
   * <p>
   * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
   * for each unique log instance.
   *
   * @param directory The log directory.
   * @throws NullPointerException If the {@code directory} is {@code null}
   */
  public void setDirectory(File directory) {
    if (directory == null)
      throw new NullPointerException("directory cannot be null");
    this.directory = directory;
  }

  /**
   * Returns the log directory.
   * <p>
   * The log will write segment files into the provided directory.
   *
   * @return The log directory. Defaults to {@code System.getProperty("user.dir")}
   */
  public File getDirectory() {
    return directory;
  }

  /**
   * Sets the log directory, returning the configuration for method chaining.
   * <p>
   * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
   * for each unique log instance.
   *
   * @param directory The log directory.
   * @return The log configuration.
   * @throws NullPointerException If the {@code directory} is {@code null}
   */
  public StorageConfig withDirectory(String directory) {
    setDirectory(directory);
    return this;
  }

  /**
   * Sets the log directory, returning the configuration for method chaining.
   * <p>
   * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
   * for each unique log instance.
   *
   * @param directory The log directory.
   * @return The log configuration.
   * @throws NullPointerException If the {@code directory} is {@code null}
   */
  public StorageConfig withDirectory(File directory) {
    setDirectory(directory);
    return this;
  }

  /**
   * Sets the maximum key size.
   * <p>
   * The maximum key size will be used to place an upper limit on the size of log segments. Keys are stored as unsigned
   * 16-bit integers and thus the key size cannot be greater than {@code Short.MAX_VALUE * 2}
   *
   * @param maxKeySize The maximum key size.
   * @throws IllegalArgumentException If the {@code maxKeySize} is not positive or is greater than
   * {@code Short.MAX_VALUE * 2}
   */
  public void setMaxKeySize(int maxKeySize) {
    if (maxKeySize <= 0)
      throw new IllegalArgumentException("maximum key size must be positive");
    if (maxKeySize > MAX_KEY_SIZE)
      throw new IllegalArgumentException("maximum key size cannot be greater than " + MAX_KEY_SIZE);
    this.maxKeySize = maxKeySize;
  }

  /**
   * Returns the maximum key size.
   * <p>
   * The maximum key size will be used to place an upper limit on the size of log segments.
   *
   * @return The maximum key size.
   */
  public int getMaxKeySize() {
    return maxKeySize;
  }

  /**
   * Sets the maximum key size, returning the configuration for method chaining.
   * <p>
   * The maximum key size will be used to place an upper limit on the size of log segments. Keys are stored as unsigned
   * 16-bit integers and thus the key size cannot be greater than {@code Short.MAX_VALUE * 2}. By default the
   * {@code maxKeySize} is {@code 1024}.
   *
   * @param maxKeySize The maximum key size.
   * @return The log configuration.
   * @throws IllegalArgumentException If the {@code maxKeySize} is not positive or is greater than
   * {@code Short.MAX_VALUE * 2}
   */
  public StorageConfig withMaxKeySize(int maxKeySize) {
    setMaxKeySize(maxKeySize);
    return this;
  }

  /**
   * Sets the maximum entry size.
   * <p>
   * The maximum entry size will be used to place an upper limit on the size of log segments.
   *
   * @param maxEntrySize The maximum key size.
   * @throws IllegalArgumentException If the {@code maxEntrySize} is not positive
   */
  public void setMaxEntrySize(int maxEntrySize) {
    if (maxEntrySize <= 0)
      throw new IllegalArgumentException("maximum entry size must be positive");
    this.maxEntrySize = maxEntrySize;
  }

  /**
   * Returns the maximum entry size.
   * <p>
   * The maximum entry size will be used to place an upper limit on the size of log segments. By default the {@code maxEntrySize}
   * is {@code 1024 * 8}
   *
   * @return The maximum entry size.
   */
  public int getMaxEntrySize() {
    return maxEntrySize;
  }

  /**
   * Sets the maximum entry size, returning the configuration for method chaining.
   * <p>
   * The maximum entry size will be used to place an upper limit on the size of log segments.
   *
   * @param maxEntrySize The maximum entry size.
   * @return The log configuration.
   * @throws IllegalArgumentException If the {@code maxEntrySize} is not positive
   */
  public StorageConfig withMaxEntrySize(int maxEntrySize) {
    setMaxEntrySize(maxEntrySize);
    return this;
  }

  /**
   * Sets the number of entries per log segment.
   * <p>
   * Because of the semantics of key deduplication and compaction, the log requires that each internal segment be of a
   * fixed number of entries. The number of entries per segment must follow the following formula:
   * {@code entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) < Integer.MAX_VALUE * 2}
   * <p>
   * By default, the number of entries per segment is {@code 1024 * 1024}.
   *
   * @param entriesPerSegment The number of entries per log segment.
   * @throws IllegalArgumentException If the number of entries per segment does not adhere to the formula
   *         {@code entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) < Integer.MAX_VALUE * 2}
   */
  public void setEntriesPerSegment(int entriesPerSegment) {
    if (entriesPerSegment <= 0)
      throw new IllegalArgumentException("entries per segment must be positive");
    if (entriesPerSegment > MAX_ENTRIES_PER_SEGMENT)
      throw new IllegalArgumentException("entries per segment cannot be greater than " + MAX_ENTRIES_PER_SEGMENT);
    if (entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) > MAX_SEGMENT_SIZE)
      throw new IllegalArgumentException("entries per segment cannot be greater than " + (MAX_SEGMENT_SIZE / (maxKeySize + maxEntrySize + Short.BYTES)));
    this.entriesPerSegment = entriesPerSegment;
  }

  /**
   * Returns the number of entries per segment.
   *
   * @return The number of entries per segment. Defaults to {@code 1024 * 1024}
   */
  public int getEntriesPerSegment() {
    return entriesPerSegment;
  }

  /**
   * Sets the number of entries per segment, returning the configuration for method chaining.
   * <p>
   * Because of the semantics of key deduplication and compaction, the log requires that each internal segment be of a
   * fixed number of entries. The number of entries per segment must follow the following formula:
   * {@code entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) < Integer.MAX_VALUE * 2}
   *
   * @param entriesPerSegment The number of entries per segment.
   * @return The log configuration.
   * @throws IllegalArgumentException If the number of entries per segment does not adhere to the formula
   *         {@code entriesPerSegment * (maxKeySize + maxEntrySize + Short.BYTES) < Integer.MAX_VALUE * 2}
   */
  public StorageConfig withEntriesPerSegment(int entriesPerSegment) {
    setEntriesPerSegment(entriesPerSegment);
    return this;
  }

  /**
   * Sets the log retention policy.
   * <p>
   * The retention policy dictates the amount of time for which a log segment should be retained. Each time the log
   * is compacted, the compaction strategy will be queried to determine whether any segments should be deleted from the
   * log. Retention policies can base their decision on time, size, or other factors.
   *
   * @param retentionPolicy The log retention policy.
   * @throws NullPointerException If the {@code retentionPolicy} is {@code null}
   */
  public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
    if (retentionPolicy == null)
      retentionPolicy = DEFAULT_RETENTION_POLICY;
    this.retentionPolicy = retentionPolicy;
  }

  /**
   * Returns the log retention policy.
   * <p>
   * The retention policy dictates the amount of time for which a log segment should be retained. Each time the log
   * is compacted, the compaction strategy will be queried to determine whether any segments should be deleted from the
   * log. Retention policies can base their decision on time, size, or other factors.
   *
   * @return The log retention policy. Defaults to {@link FullRetentionPolicy} which
   *         retains logs forever.
   */
  public RetentionPolicy getRetentionPolicy() {
    return retentionPolicy;
  }

  /**
   * Sets the log retention policy, returning the configuration for method chaining.
   * <p>
   * The retention policy dictates the amount of time for which a log segment should be retained. Each time the log
   * is compacted, the compaction strategy will be queried to determine whether any segments should be deleted from the
   * log. Retention policies can base their decision on time, size, or other factors.
   *
   * @param retentionPolicy The log retention policy.
   * @return The log configuration.
   * @throws NullPointerException If the {@code retentionPolicy} is {@code null}
   */
  public StorageConfig withRetentionPolicy(RetentionPolicy retentionPolicy) {
    setRetentionPolicy(retentionPolicy);
    return this;
  }

  /**
   * Sets the log compaction strategy.
   * <p>
   * The compaction strategy determined how segments of the log are compacted. Compaction is the process of removing
   * duplicate keys within a single segment or across many segments and ultimately combining multiple segments together.
   * This leads to reduced disk space usage and allows entries to be continuously appended to the log.
   * <p>
   * If the {@code compactionStrategy} is {@code null} then the default
   * {@link LeveledCompactionStrategy} will be used.
   *
   * @param compactionStrategy The log compaction strategy.
   */
  public void setCompactionStrategy(CompactionStrategy compactionStrategy) {
    if (compactionStrategy == null)
      compactionStrategy = DEFAULT_COMPACTION_STRATEGY;
    this.compactionStrategy = compactionStrategy;
  }

  /**
   * Returns the log compaction strategy.
   * <p>
   * The compaction strategy determined how segments of the log are compacted. Compaction is the process of removing
   * duplicate keys within a single segment or across many segments and ultimately combining multiple segments together.
   * This leads to reduced disk space usage and allows entries to be continuously appended to the log.
   *
   * @return The log compaction strategy. Defaults to {@link LeveledCompactionStrategy}
   */
  public CompactionStrategy getCompactionStrategy() {
    return compactionStrategy;
  }

  /**
   * Sets the log compaction strategy, returning the configuration for method chaining.
   * <p>
   * The compaction strategy determined how segments of the log are compacted. Compaction is the process of removing
   * duplicate keys within a single segment or across many segments and ultimately combining multiple segments together.
   * This leads to reduced disk space usage and allows entries to be continuously appended to the log.
   * <p>
   * If the {@code compactionStrategy} is {@code null} then the default
   * {@link LeveledCompactionStrategy} will be used.
   *
   * @param compactionStrategy The log compaction strategy.
   * @return The log configuration.
   */
  public StorageConfig withCompactionStrategy(CompactionStrategy compactionStrategy) {
    setCompactionStrategy(compactionStrategy);
    return this;
  }

  /**
   * Sets the log compact interval.
   * <p>
   * A background thread will periodically attempt to compact the log at the specified interval using the configured
   * {@link CompactionStrategy}
   *
   * @param interval The log compact interval.
   * @param unit The interval time unit.
   * @throws IllegalArgumentException If the {@code compactInterval} is negative
   */
  public void setCompactInterval(long interval, TimeUnit unit) {
    setCompactInterval(unit.toMillis(interval));
  }

  /**
   * Sets the log compact interval.
   * <p>
   * A background thread will periodically attempt to compact the log at the specified interval using the configured
   * {@link CompactionStrategy}
   *
   * @param compactInterval The log compact interval in milliseconds.
   * @throws IllegalArgumentException If the {@code compactInterval} is negative
   */
  public void setCompactInterval(long compactInterval) {
    if (compactInterval <= 0)
      throw new IllegalArgumentException("compact interval must be positive");
    this.compactInterval = compactInterval;
  }

  /**
   * Returns the log compact interval.
   * <p>
   * The compact interval defines the number of milliseconds between log compaction attempts.
   *
   * @return The log compact interval.
   */
  public long getCompactInterval() {
    return compactInterval;
  }

  /**
   * Sets the log compact interval, returning the configuration for method chaining.
   * <p>
   * A background thread will periodically attempt to compact the log at the specified interval using the configured
   * {@link CompactionStrategy}
   *
   * @param interval The log compact interval.
   * @param unit The interval time unit.
   * @return The log configuration.
   * @throws IllegalArgumentException If the {@code compactInterval} is negative
   */
  public StorageConfig withCompactInterval(long interval, TimeUnit unit) {
    return withCompactInterval(unit.toMillis(interval));
  }

  /**
   * Sets the log compact interval, returning the configuration for method chaining.
   * <p>
   * A background thread will periodically attempt to compact the log at the specified interval using the configured
   * {@link CompactionStrategy}
   *
   * @param compactInterval The log compact interval in milliseconds.
   * @return The log configuration.
   * @throws IllegalArgumentException If the {@code compactInterval} is negative
   */
  public StorageConfig withCompactInterval(long compactInterval) {
    setCompactInterval(compactInterval);
    return this;
  }

}
