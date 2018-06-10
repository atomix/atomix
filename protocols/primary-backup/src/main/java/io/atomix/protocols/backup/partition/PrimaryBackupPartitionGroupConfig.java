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
package io.atomix.protocols.backup.partition;

import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;

/**
 * Primary-backup partition group configuration.
 */
public class PrimaryBackupPartitionGroupConfig extends PartitionGroupConfig<PrimaryBackupPartitionGroupConfig> {
  private static final int DEFAULT_PARTITIONS = 71;

  private String memberGroupStrategy = MemberGroupStrategy.NODE_AWARE.name();

  @Override
  public PartitionGroup.Type getType() {
    return PrimaryBackupPartitionGroup.TYPE;
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
  public PrimaryBackupPartitionGroupConfig setMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
    this.memberGroupStrategy = memberGroupStrategy.name();
    return this;
  }
}
