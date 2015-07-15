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
public class LogConfig {
  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 8;
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = (int) (Math.pow(2, 31) - 1) / 8 - 16;
  private static final long DEFAULT_MINOR_COMPACTION_INTERVAL = TimeUnit.MINUTES.toMillis(1);
  private static final long DEFAULT_MAJOR_COMPACTION_INTERVAL = TimeUnit.HOURS.toMillis(1);

  private File directory = new File(DEFAULT_DIRECTORY);
  private StorageLevel level = StorageLevel.DISK;
  private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  private int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;
  private long minorCompactionInterval = DEFAULT_MINOR_COMPACTION_INTERVAL;
  private long majorCompactionInterval = DEFAULT_MAJOR_COMPACTION_INTERVAL;

  public LogConfig() {
  }

  private LogConfig(LogConfig config) {
    directory = config.directory;
    level = config.level;
    maxEntrySize = config.maxEntrySize;
    maxSegmentSize = config.maxSegmentSize;
    maxEntriesPerSegment = config.maxEntriesPerSegment;
  }

  /**
   * Copies the log configuration.
   */
  LogConfig copy() {
    return new LogConfig(this);
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
  public LogConfig withDirectory(String directory) {
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
  public LogConfig withDirectory(File directory) {
    setDirectory(directory);
    return this;
  }

  /**
   * Sets the log storage level.
   *
   * @param level The log storage level.
   * @throws java.lang.NullPointerException If the {@code level} is {@code null}
   */
  public void setStorageLevel(StorageLevel level) {
    if (level == null)
      throw new NullPointerException("level cannot be null");
    this.level = level;
  }

  /**
   * Returns the log storage level.
   *
   * @return The log storage level.
   */
  public StorageLevel getStorageLevel() {
    return level;
  }

  /**
   * Sets the log storage level, returning the configuration for method chaining.
   *
   * @param level The log storage level.
   * @return The log configuration.
   */
  public LogConfig withStorageLevel(StorageLevel level) {
    setStorageLevel(level);
    return this;
  }

  /**
   * Sets the maximum entry size.
   * <p>
   * The maximum entry size will be used to place an upper limit on the size of log segments.
   *
   * @param maxEntrySize The maximum key size.
   * @throws IllegalArgumentException If the {@code maxEntrySize} is not positive or is greater than
   * {@link LogConfig#getMaxSegmentSize()}
   */
  public void setMaxEntrySize(int maxEntrySize) {
    if (maxEntrySize <= 0)
      throw new IllegalArgumentException("maximum entry size must be positive");
    if (maxEntrySize > maxSegmentSize)
      throw new IllegalArgumentException("maximum entry size must be less than maximum segment size");
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
  public LogConfig withMaxEntrySize(int maxEntrySize) {
    setMaxEntrySize(maxEntrySize);
    return this;
  }

  /**
   * Sets the maximum segment size.
   *
   * @param maxSegmentSize The maximum segment size.
   * @throws java.lang.IllegalArgumentException If the segment size is not positive or is less than
   * {@link LogConfig#getMaxEntrySize()}.
   */
  public void setMaxSegmentSize(int maxSegmentSize) {
    if (maxSegmentSize <= 0)
      throw new IllegalArgumentException("maximum segment size must be positive");
    if (maxSegmentSize < maxEntrySize)
      throw new IllegalArgumentException("maximum segment size must be greater than maxEntrySize");
    this.maxSegmentSize = maxSegmentSize;
  }

  /**
   * Returns the maximum log segment size.
   *
   * @return The maximum log segment size.
   */
  public int getMaxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Sets the maximum log segment size, returning the configuration for method chaining.
   *
   * @param maxSegmentSize The maximum segment size.
   * @return The log configuration.
   */
  public LogConfig withMaxSegmentSize(int maxSegmentSize) {
    setMaxSegmentSize(maxSegmentSize);
    return this;
  }

  /**
   * Sets the maximum number of allowed entries per segment.
   *
   * @param maxEntriesPerSegment The maximum number of allowed entries per segment.
   * @throws java.lang.IllegalArgumentException If the maximum number of entries per segment is greater than
   *         {@code (int) (Math.pow(2, 31) - 1) / 8}
   */
  public void setMaxEntriesPerSegment(int maxEntriesPerSegment) {
    if (maxEntriesPerSegment > DEFAULT_MAX_ENTRIES_PER_SEGMENT)
      throw new IllegalArgumentException("max entries per segment cannot be greater than " + DEFAULT_MAX_ENTRIES_PER_SEGMENT);
    this.maxEntriesPerSegment = maxEntriesPerSegment;
  }

  /**
   * Returns the maximum number of entries per segment.
   *
   * @return The maximum number of entries per segment.
   */
  public int getMaxEntriesPerSegment() {
    return maxEntriesPerSegment;
  }

  /**
   * Sets the maximum number of allowed entries per segment, returning the configuration for method chaining.
   *
   * @param maxEntriesPerSegment The maximum number of allowed entries per segment.
   * @return The log configuration.
   * @throws java.lang.IllegalArgumentException If the maximum number of entries per segment is greater than
   *         {@code (int) (Math.pow(2, 31) - 1) / 8}
   */
  public LogConfig withMaxEntriesPerSegment(int maxEntriesPerSegment) {
    setMaxEntriesPerSegment(maxEntriesPerSegment);
    return this;
  }

  public void setMinorCompactionInterval(long compactionInterval) {
    if (compactionInterval <= 0)
      throw new IllegalArgumentException("compaction interval must be positive");
    this.minorCompactionInterval = compactionInterval;
  }

  public void setMinorCompactionInterval(long compactionInterval, TimeUnit unit) {
    setMinorCompactionInterval(unit.toMillis(compactionInterval));
  }

  public long getMinorCompactionInterval() {
    return minorCompactionInterval;
  }

  public void setMajorCompactionInterval(long compactionInterval) {
    if (compactionInterval <= 0)
      throw new IllegalArgumentException("compaction interval must be positive");
    this.majorCompactionInterval = compactionInterval;
  }

  public void setMajorCompactionInterval(long compactionInterval, TimeUnit unit) {
    setMajorCompactionInterval(unit.toMillis(compactionInterval));
  }

  public long getMajorCompactionInterval() {
    return majorCompactionInterval;
  }

}
