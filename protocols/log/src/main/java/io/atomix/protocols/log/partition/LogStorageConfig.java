/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.log.partition;

import io.atomix.storage.StorageLevel;
import io.atomix.utils.memory.MemorySize;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log storage configuration.
 */
public class LogStorageConfig {
  private static final String DATA_PREFIX = ".data";
  private static final StorageLevel DEFAULT_STORAGE_LEVEL = StorageLevel.MAPPED;
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 1024;
  private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;

  private String directory;
  private StorageLevel level = DEFAULT_STORAGE_LEVEL;
  private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  private long segmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;

  /**
   * Returns the partition storage level.
   *
   * @return the partition storage level
   */
  public StorageLevel getLevel() {
    return level;
  }

  /**
   * Sets the partition storage level.
   *
   * @param storageLevel the partition storage level
   * @return the log partition group configuration
   */
  public LogStorageConfig setLevel(StorageLevel storageLevel) {
    this.level = checkNotNull(storageLevel);
    return this;
  }

  /**
   * Returns the log segment size.
   *
   * @return the log segment size
   */
  public MemorySize getSegmentSize() {
    return MemorySize.from(segmentSize);
  }

  /**
   * Sets the log segment size.
   *
   * @param segmentSize the log segment size
   * @return the partition group configuration
   */
  public LogStorageConfig setSegmentSize(MemorySize segmentSize) {
    this.segmentSize = segmentSize.bytes();
    return this;
  }

  /**
   * Returns the maximum entry size.
   *
   * @return the maximum entry size
   */
  public MemorySize getMaxEntrySize() {
    return MemorySize.from(maxEntrySize);
  }

  /**
   * Sets the maximum entry size.
   *
   * @param maxEntrySize the maximum entry size
   * @return the log storage configuration
   */
  public LogStorageConfig setMaxEntrySize(MemorySize maxEntrySize) {
    this.maxEntrySize = (int) maxEntrySize.bytes();
    return this;
  }

  /**
   * Returns whether to flush logs to disk on commit.
   *
   * @return whether to flush logs to disk on commit
   */
  public boolean isFlushOnCommit() {
    return flushOnCommit;
  }

  /**
   * Sets whether to flush logs to disk on commit.
   *
   * @param flushOnCommit whether to flush logs to disk on commit
   * @return the log partition group configuration
   */
  public LogStorageConfig setFlushOnCommit(boolean flushOnCommit) {
    this.flushOnCommit = flushOnCommit;
    return this;
  }

  /**
   * Returns the partition data directory.
   *
   * @param groupName the partition group name
   * @return the partition data directory
   */
  public String getDirectory(String groupName) {
    return directory != null ? directory : System.getProperty("atomix.data", DATA_PREFIX) + "/" + groupName;
  }

  /**
   * Sets the partition data directory.
   *
   * @param directory the partition data directory
   * @return the log partition group configuration
   */
  public LogStorageConfig setDirectory(String directory) {
    this.directory = directory;
    return this;
  }
}
