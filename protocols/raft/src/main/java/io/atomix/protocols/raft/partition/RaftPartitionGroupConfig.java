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

import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.storage.StorageLevel;

import java.io.File;

/**
 * Raft partition group configuration.
 */
public class RaftPartitionGroupConfig extends PartitionGroupConfig<RaftPartitionGroupConfig> {
  private static final int DEFAULT_PARTITIONS = 7;

  private int partitionSize;
  private StorageLevel storageLevel = StorageLevel.MAPPED;
  private File dataDirectory = new File(System.getProperty("user.dir"), "data");

  @Override
  protected int getDefaultPartitions() {
    return DEFAULT_PARTITIONS;
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
  public StorageLevel getStorageLevel() {
    return storageLevel;
  }

  /**
   * Sets the partition storage level.
   *
   * @param storageLevel the partition storage level
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setStorageLevel(StorageLevel storageLevel) {
    this.storageLevel = storageLevel;
    return this;
  }

  /**
   * Returns the partition data directory.
   *
   * @return the partition data directory
   */
  public File getDataDirectory() {
    return dataDirectory;
  }

  /**
   * Sets the partition data directory.
   *
   * @param dataDirectory the partition data directory
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setDataDirectory(String dataDirectory) {
    return setDataDirectory(new File(dataDirectory));
  }

  /**
   * Sets the partition data directory.
   *
   * @param dataDirectory the partition data directory
   * @return the Raft partition group configuration
   */
  public RaftPartitionGroupConfig setDataDirectory(File dataDirectory) {
    this.dataDirectory = dataDirectory;
    return this;
  }
}
