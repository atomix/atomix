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
public class DataGridProfile implements NamedProfile {
  private static final String NAME = "data-grid";

  private static final String SYSTEM_GROUP_NAME = "system";
  private static final String GROUP_NAME = "data";
  private static final int NUM_PARTITIONS = 71;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void configure(AtomixConfig config) {
    if (config.getSystemPartitionGroup() == null) {
      config.setSystemPartitionGroup(new PrimaryBackupPartitionGroupConfig()
          .setName(SYSTEM_GROUP_NAME)
          .setPartitions(1)
          .setMemberGroupStrategy(MemberGroupStrategy.RACK_AWARE));
    }
    config.addPartitionGroup(new PrimaryBackupPartitionGroupConfig()
        .setName(GROUP_NAME)
        .setPartitions(NUM_PARTITIONS)
        .setMemberGroupStrategy(MemberGroupStrategy.RACK_AWARE));
  }
}
