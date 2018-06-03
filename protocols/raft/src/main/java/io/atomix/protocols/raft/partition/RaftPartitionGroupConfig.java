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
package io.atomix.protocols.raft.partition;

import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.memory.MemorySize;

import java.util.HashSet;
import java.util.Set;

/**
 * Raft partition group configuration.
 */
public class RaftPartitionGroupConfig extends PartitionGroupConfig<RaftPartitionGroupConfig> {
  private static final int DEFAULT_PARTITIONS = 7;
  private static final String DATA_PREFIX = ".data";

  private Set<String> members = new HashSet<>();
  private int partitionSize;
  private String storageLevel = StorageLevel.MAPPED.name();
  private long segmentSize = 1024 * 1024 * 32;
  private boolean flushOnCommit = true;
  private String dataDirectory;

  @Override
  public PartitionGroup.Type getType() {
    return RaftPartitionGroup.TYPE;
  }

  @Override
  protected int getDefaultPartitions() {
    return DEFAULT_PARTITIONS;
  }

  /**
   * Returns the set of members in the partition group.
   *
   * @return the set of members in the partition group
   */
  public Set<String> getMembers() {
    return members;
  }

  /**
   * Sets the set of members in the partition group.
   *
   * @param members the set of members in the partition group
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setMembers(Set<String> members) {
    this.members = members;
    return this;
  }

  /**
   * Returns the partition size.
   *
   * @return the partition size
   */
  public int getPartitionSize() {
    return partitionSize;
  }

  /**
   * Sets the partition size.
   *
   * @param partitionSize the partition size
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setPartitionSize(int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  /**
   * Returns the partition storage level.
   *
   * @return the partition storage level
   */
  public String getStorageLevel() {
    return storageLevel;
  }

  /**
   * Sets the partition storage level.
   *
   * @param storageLevel the partition storage level
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setStorageLevel(String storageLevel) {
    StorageLevel.valueOf(storageLevel.toUpperCase());
    this.storageLevel = storageLevel;
    return this;
  }

  /**
   * Returns the Raft log segment size.
   *
   * @return the Raft log segment size
   */
  public MemorySize getSegmentSize() {
    return MemorySize.from(segmentSize);
  }

  /**
   * Sets the Raft log segment size.
   *
   * @param segmentSize the Raft log segment size
   * @return the partition group configuration
   */
  public RaftPartitionGroupConfig setSegmentSize(MemorySize segmentSize) {
    this.segmentSize = segmentSize.bytes();
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
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setFlushOnCommit(boolean flushOnCommit) {
    this.flushOnCommit = flushOnCommit;
    return this;
  }

  /**
   * Returns the partition data directory.
   *
   * @return the partition data directory
   */
  public String getDataDirectory() {
    return dataDirectory != null ? dataDirectory : DATA_PREFIX + "/" + getName();
  }

  /**
   * Sets the partition data directory.
   *
   * @param dataDirectory the partition data directory
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setDataDirectory(String dataDirectory) {
    this.dataDirectory = dataDirectory;
    return this;
  }
}
