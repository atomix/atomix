// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Consensus profile configuration.
 */
public class ConsensusProfileConfig extends ProfileConfig {
  private String dataPath = System.getProperty("atomix.data", ".data");
  private String managementGroup = "system";
  private String dataGroup = "raft";
  private int partitionSize = 3;
  private int partitions = 7;
  private Set<String> members = Collections.emptySet();

  @Override
  public Profile.Type getType() {
    return ConsensusProfile.TYPE;
  }

  /**
   * Returns the data file path.
   *
   * @return the consensus data file path
   */
  public String getDataPath() {
    return dataPath;
  }

  /**
   * Sets the consensus data file path.
   *
   * @param dataPath the consensus data file path
   * @return the consensus profile configuration
   */
  public ConsensusProfileConfig setDataPath(String dataPath) {
    this.dataPath = checkNotNull(dataPath);
    return this;
  }

  /**
   * Returns the management partition group name.
   *
   * @return the management partition group name
   */
  public String getManagementGroup() {
    return managementGroup;
  }

  /**
   * Sets the management partition group name.
   *
   * @param managementGroup the management partition group name
   * @return the consensus profile configurations
   */
  public ConsensusProfileConfig setManagementGroup(String managementGroup) {
    this.managementGroup = checkNotNull(managementGroup);
    return this;
  }

  /**
   * Returns the data partition group name.
   *
   * @return the data partition group name
   */
  public String getDataGroup() {
    return dataGroup;
  }

  /**
   * Sets the data partition group name.
   *
   * @param dataGroup the data partition group name
   * @return the consensus profile configurations
   */
  public ConsensusProfileConfig setDataGroup(String dataGroup) {
    this.dataGroup = checkNotNull(dataGroup);
    return this;
  }

  /**
   * Returns the data partition size.
   *
   * @return the data partition size
   */
  public int getPartitionSize() {
    return partitionSize;
  }

  /**
   * Sets the data partition size.
   *
   * @param partitionSize the data partition size
   * @return the consensus profile configurations
   */
  public ConsensusProfileConfig setPartitionSize(int partitionSize) {
    checkArgument(partitionSize > 0, "partitionSize must be positive");
    this.partitionSize = partitionSize;
    return this;
  }

  /**
   * Returns the number of data partitions to configure.
   *
   * @return the number of data partitions to configure
   */
  public int getPartitions() {
    return partitions;
  }

  /**
   * Sets the number of data partitions to configure.
   *
   * @param partitions the number of data partitions to configure
   * @return the consensus profile configurations
   */
  public ConsensusProfileConfig setPartitions(int partitions) {
    checkArgument(partitions > 0, "partitions must be positive");
    this.partitions = partitions;
    return this;
  }

  /**
   * Returns the consensus members.
   *
   * @return the consensus members
   */
  public Set<String> getMembers() {
    return members;
  }

  /**
   * Sets the consensus members.
   *
   * @param members the consensus members
   * @return the profile configuration
   */
  public ConsensusProfileConfig setMembers(Set<String> members) {
    this.members = members;
    return this;
  }
}
