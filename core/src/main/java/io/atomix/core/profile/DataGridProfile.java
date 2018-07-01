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

import io.atomix.core.AtomixConfig;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroupConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In-memory data grid profile.
 */
public class DataGridProfile implements Profile {
  public static final Type TYPE = new Type();

  /**
   * Creates a new data grid profile builder.
   *
   * @return a new data grid profile builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Data-grid profile type.
   */
  public static class Type implements Profile.Type<Config> {
    private static final String NAME = "data-grid";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Config newConfig() {
      return new Config();
    }

    @Override
    public Profile newProfile(Config config) {
      return new DataGridProfile(config);
    }
  }

  /**
   * Data grid profile builder.
   */
  public static class Builder implements Profile.Builder {
    private final Config config = new Config();

    private Builder() {
    }

    /**
     * Sets the management partition group name.
     *
     * @param managementGroup the management partition group name
     * @return the data grid profile builder
     */
    public Builder withManagementGroup(String managementGroup) {
      config.setManagementGroup(managementGroup);
      return this;
    }

    /**
     * Sets the data partition group name.
     *
     * @param dataGroup the data partition group name
     * @return the data grid profile builder
     */
    public Builder withDataGroup(String dataGroup) {
      config.setDataGroup(dataGroup);
      return this;
    }

    /**
     * Sets the number of data partitions.
     *
     * @param numPartitions the number of data partitions
     * @return the data grid profile builder
     */
    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    @Override
    public Profile build() {
      return new DataGridProfile(config);
    }
  }

  /**
   * Data grid profile configuration.
   */
  public static class Config implements Profile.Config {
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
    public Config setManagementGroup(String managementGroup) {
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
    public Config setDataGroup(String dataGroup) {
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
    public Config setPartitions(int partitions) {
      this.partitions = partitions;
      return this;
    }
  }

  private final Config config;

  DataGridProfile() {
    this(new Config());
  }

  DataGridProfile(int numPartitions) {
    this(new Config().setPartitions(numPartitions));
  }

  private DataGridProfile(Config config) {
    this.config = config;
  }

  @Override
  public Profile.Config config() {
    return config;
  }

  @Override
  public void configure(AtomixConfig config) {
    if (config.getManagementGroup() == null) {
      config.setManagementGroup(new PrimaryBackupPartitionGroupConfig()
          .setName(this.config.getManagementGroup())
          .setPartitions(1)
          .setMemberGroupStrategy(MemberGroupStrategy.RACK_AWARE));
    }
    config.addPartitionGroup(new PrimaryBackupPartitionGroupConfig()
        .setName(this.config.getDataGroup())
        .setPartitions(this.config.getPartitions())
        .setMemberGroupStrategy(MemberGroupStrategy.RACK_AWARE));
  }
}
