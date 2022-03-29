// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
