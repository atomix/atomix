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
