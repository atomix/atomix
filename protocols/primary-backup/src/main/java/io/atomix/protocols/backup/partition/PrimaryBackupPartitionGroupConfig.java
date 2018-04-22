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

import io.atomix.primitive.partition.MemberFilter;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.PartitionGroupConfig;

/**
 * Primary-backup partition group configuration.
 */
public class PrimaryBackupPartitionGroupConfig extends PartitionGroupConfig<PrimaryBackupPartitionGroupConfig> {
  private static final int DEFAULT_PARTITIONS = 71;

  private MemberFilter memberFilter = node -> true;
  private MemberGroupProvider memberGroupProvider = MemberGroupStrategy.NODE_AWARE;

  @Override
  protected int getDefaultPartitions() {
    return DEFAULT_PARTITIONS;
  }

  /**
   * Returns the member filter.
   *
   * @return the member filter
   */
  public MemberFilter getMemberFilter() {
    return memberFilter;
  }

  /**
   * Sets the member filter.
   *
   * @param memberFilter the member filter
   * @return the primary backup partition group configuration
   */
  public PrimaryBackupPartitionGroupConfig setMemberFilter(MemberFilter memberFilter) {
    this.memberFilter = memberFilter;
    return this;
  }

  /**
   * Returns the member group provider.
   *
   * @return the member group provider
   */
  public MemberGroupProvider getMemberGroupProvider() {
    return memberGroupProvider;
  }

  /**
   * Sets the member group provider.
   *
   * @param memberGroupProvider the member group provider
   * @return the partition group configuration
   */
  public PrimaryBackupPartitionGroupConfig setMemberGroupProvider(MemberGroupProvider memberGroupProvider) {
    this.memberGroupProvider = memberGroupProvider;
    return this;
  }

  /**
   * Sets the member group strategy.
   *
   * @param memberGroupStrategy the member group strategy
   * @return the partition group configuration
   */
  public PrimaryBackupPartitionGroupConfig setMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
    return setMemberGroupProvider(memberGroupStrategy);
  }
}
