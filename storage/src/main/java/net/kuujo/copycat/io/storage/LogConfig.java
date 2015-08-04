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

import java.io.File;

/**
 * Copycat storage configuration.
 * <p>
 * The storage configuration is used to create log files, allocate disk space and predict disk usage for each segment in
 * the log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LogConfig {
  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 8;
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = (int) (Math.pow(2, 31) - 1) / 8 - 16;
  private static final int DEFAULT_CLEANER_THREADS = 4;

  private File directory = new File(DEFAULT_DIRECTORY);
  private StorageLevel level = StorageLevel.DISK;
  private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  private int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;
  private int cleanerThreads = DEFAULT_CLEANER_THREADS;

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
   * Sets the maximum entry count.
   * <p>
   * The maximum entry count will be used to place an upper limit on the count of log segments.
   *
   * @param maxEntrySize The maximum key count.
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
   * Returns the maximum entry count.
   * <p>
   * The maximum entry count will be used to place an upper limit on the count of log segments. By default the {@code maxEntrySize}
   * is {@code 1024 * 8}
   *
   * @return The maximum entry count.
   */
  public int getMaxEntrySize() {
    return maxEntrySize;
  }

  /**
   * Sets the maximum segment count.
   *
   * @param maxSegmentSize The maximum segment count.
   * @throws java.lang.IllegalArgumentException If the segment count is not positive or is less than
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
   * Returns the maximum log segment count.
   *
   * @return The maximum log segment count.
   */
  public int getMaxSegmentSize() {
    return maxSegmentSize;
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
   * Sets the number of cleaner threads.
   *
   * @param cleanerThreads The number of cleaner threads.
   */
  public void setCleanerThreads(int cleanerThreads) {
    if (cleanerThreads <= 0)
      throw new IllegalArgumentException("cleanerThreads must be positive");
    this.cleanerThreads = cleanerThreads;
  }

  /**
   * Returns the number of cleaner threads.
   *
   * @return The number of cleaner threads.
   */
  public int getCleanerThreads() {
    return cleanerThreads;
  }

}
