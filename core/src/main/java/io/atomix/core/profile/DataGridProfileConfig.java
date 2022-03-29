// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.core.profile.DataGridProfile.TYPE;

/**
 * Data grid profile configuration.
 */
public class DataGridProfileConfig extends ProfileConfig {
  private String managementGroup = "system";
  private String dataGroup = "data";
  private int partitions = 71;

  @Override
  public Profile.Type getType() {
    return TYPE;
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
   * @return the data grid profile configuration
   */
  public DataGridProfileConfig setManagementGroup(String managementGroup) {
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
   * @return the data grid profile configuration
   */
  public DataGridProfileConfig setDataGroup(String dataGroup) {
    this.dataGroup = checkNotNull(dataGroup);
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
   * @return the data grid profile configuration
   */
  public DataGridProfileConfig setPartitions(int partitions) {
    this.partitions = partitions;
    return this;
  }
}
