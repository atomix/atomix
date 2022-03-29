// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.profile;

/**
 * Data grid profile builder.
 */
public class DataGridProfileBuilder extends ProfileBuilder {
  private final DataGridProfileConfig config = new DataGridProfileConfig();

  DataGridProfileBuilder() {
  }

  /**
   * Sets the management partition group name.
   *
   * @param managementGroup the management partition group name
   * @return the data grid profile builder
   */
  public DataGridProfileBuilder withManagementGroup(String managementGroup) {
    config.setManagementGroup(managementGroup);
    return this;
  }

  /**
   * Sets the data partition group name.
   *
   * @param dataGroup the data partition group name
   * @return the data grid profile builder
   */
  public DataGridProfileBuilder withDataGroup(String dataGroup) {
    config.setDataGroup(dataGroup);
    return this;
  }

  /**
   * Sets the number of data partitions.
   *
   * @param numPartitions the number of data partitions
   * @return the data grid profile builder
   */
  public DataGridProfileBuilder withNumPartitions(int numPartitions) {
    config.setPartitions(numPartitions);
    return this;
  }

  @Override
  public Profile build() {
    return new DataGridProfile(config);
  }
}
