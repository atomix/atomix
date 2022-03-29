// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

import com.google.common.collect.Sets;

import java.io.File;
import java.util.Set;

/**
 * Consensus profile builder.
 */
public class ConsensusProfileBuilder extends ProfileBuilder {
  private final ConsensusProfileConfig config = new ConsensusProfileConfig();

  ConsensusProfileBuilder() {
  }

  /**
   * Sets the consensus data file path.
   *
   * @param dataPath the consensus data file path
   * @return the consensus profile builder
   */
  public ConsensusProfileBuilder withDataPath(String dataPath) {
    config.setDataPath(dataPath);
    return this;
  }

  /**
   * Sets the consensus data file path.
   *
   * @param dataPath the consensus data file path
   * @return the consensus profile builder
   */
  public ConsensusProfileBuilder withDataPath(File dataPath) {
    config.setDataPath(dataPath.getPath());
    return this;
  }

  /**
   * Sets the management partition group name.
   *
   * @param managementGroup the management partition group name
   * @return the consensus profile builder
   */
  public ConsensusProfileBuilder withManagementGroup(String managementGroup) {
    config.setManagementGroup(managementGroup);
    return this;
  }

  /**
   * Sets the data partition group name.
   *
   * @param dataGroup the data partition group name
   * @return the consensus profile builder
   */
  public ConsensusProfileBuilder withDataGroup(String dataGroup) {
    config.setDataGroup(dataGroup);
    return this;
  }

  /**
   * Sets the data partition size.
   *
   * @param partitionSize the data partition size
   * @return the consensus profile builder
   */
  public ConsensusProfileBuilder withPartitionSize(int partitionSize) {
    config.setPartitionSize(partitionSize);
    return this;
  }

  /**
   * Sets the number of data partitions to configure.
   *
   * @param numPartitions the number of data partitions to configure
   * @return the consensus profile builder
   */
  public ConsensusProfileBuilder withNumPartitions(int numPartitions) {
    config.setPartitions(numPartitions);
    return this;
  }

  /**
   * Sets the consensus members.
   *
   * @param members the consensus members
   * @return the profile builder
   */
  public ConsensusProfileBuilder withMembers(String... members) {
    return withMembers(Sets.newHashSet(members));
  }

  /**
   * Sets the consensus members.
   *
   * @param members the consensus members
   * @return the profile builder
   */
  public ConsensusProfileBuilder withMembers(Set<String> members) {
    config.setMembers(members);
    return this;
  }

  @Override
  public Profile build() {
    return new ConsensusProfile(config);
  }
}
