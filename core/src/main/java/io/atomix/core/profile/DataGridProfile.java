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
  public static DataGridProfileBuilder builder() {
    return new DataGridProfileBuilder();
  }

  /**
   * Data-grid profile type.
   */
  public static class Type implements Profile.Type<DataGridProfileConfig> {
    private static final String NAME = "data-grid";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public DataGridProfileConfig newConfig() {
      return new DataGridProfileConfig();
    }

    @Override
    public Profile newProfile(DataGridProfileConfig config) {
      return new DataGridProfile(config);
    }
  }

  private final DataGridProfileConfig config;

  DataGridProfile() {
    this(new DataGridProfileConfig());
  }

  DataGridProfile(int numPartitions) {
    this(new DataGridProfileConfig().setPartitions(numPartitions));
  }

  DataGridProfile(DataGridProfileConfig config) {
    this.config = config;
  }

  @Override
  public DataGridProfileConfig config() {
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
