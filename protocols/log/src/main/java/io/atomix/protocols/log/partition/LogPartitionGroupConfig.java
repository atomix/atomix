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

import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log partition group configuration.
 */
public class LogPartitionGroupConfig extends PartitionGroupConfig<LogPartitionGroupConfig> {
  private static final int DEFAULT_PARTITIONS = 7;

  private String memberGroupStrategy = MemberGroupStrategy.NODE_AWARE.name();
  private LogStorageConfig storageConfig = new LogStorageConfig();
  private LogCompactionConfig compactionConfig = new LogCompactionConfig();

  @Override
  public PartitionGroup.Type getType() {
    return LogPartitionGroup.TYPE;
  }

  @Override
  protected int getDefaultPartitions() {
    return DEFAULT_PARTITIONS;
  }

  /**
   * Returns the member group provider.
   *
   * @return the member group provider
   */
  public MemberGroupStrategy getMemberGroupProvider() {
    return MemberGroupStrategy.valueOf(memberGroupStrategy);
  }

  /**
   * Sets the member group strategy.
   *
   * @param memberGroupStrategy the member group strategy
   * @return the partition group configuration
   */
  public LogPartitionGroupConfig setMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
    this.memberGroupStrategy = memberGroupStrategy.name();
    return this;
  }

  /**
   * Returns the log storage configuration.
   *
   * @return the log storage configuration
   */
  public LogStorageConfig getStorageConfig() {
    return storageConfig;
  }

  /**
   * Sets the log storage configuration.
   *
   * @param storageConfig the log storage configuration
   * @return the partition group configuration
   */
  public LogPartitionGroupConfig setStorageConfig(LogStorageConfig storageConfig) {
    this.storageConfig = checkNotNull(storageConfig, "storageConfig cannot be null");
    return this;
  }

  /**
   * Returns the log compaction configuration.
   *
   * @return the log compaction configuration
   */
  public LogCompactionConfig getCompactionConfig() {
    return compactionConfig;
  }

  /**
   * Sets the log compaction configuration.
   *
   * @param compactionConfig the log compaction configuration
   * @return the partition group configuration
   */
  public LogPartitionGroupConfig setCompactionConfig(LogCompactionConfig compactionConfig) {
    this.compactionConfig = checkNotNull(compactionConfig, "compactionConfig cannot be null");
    return this;
  }
}
