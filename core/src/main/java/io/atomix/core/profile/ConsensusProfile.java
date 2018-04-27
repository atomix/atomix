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

import io.atomix.cluster.Member;
import io.atomix.core.AtomixConfig;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;

import java.util.stream.Collectors;

/**
 * Consensus profile.
 */
public class ConsensusProfile implements NamedProfile {
  private static final String NAME = "consensus";

  private static final String DATA_PATH = ".data";
  private static final String SYSTEM_GROUP_NAME = "system";
  private static final String GROUP_NAME = "raft";
  private static final int PARTITION_SIZE = 3;
  private static final int NUM_PARTITIONS = 7;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void configure(AtomixConfig config) {
    config.setManagementGroup(new RaftPartitionGroupConfig()
        .setName(SYSTEM_GROUP_NAME)
        .setPartitionSize((int) config.getClusterConfig().getMembers()
            .values()
            .stream()
            .filter(node -> node.getType() == Member.Type.PERSISTENT)
            .count())
        .setPartitions(1)
        .setMembers(config.getClusterConfig().getMembers()
            .values()
            .stream()
            .filter(node -> node.getType() == Member.Type.PERSISTENT)
            .map(node -> node.getId().id())
            .collect(Collectors.toSet()))
        .setDataDirectory(String.format("%s/%s", DATA_PATH, SYSTEM_GROUP_NAME)));
    config.addPartitionGroup(new RaftPartitionGroupConfig()
        .setName(GROUP_NAME)
        .setPartitionSize(PARTITION_SIZE)
        .setPartitions(NUM_PARTITIONS)
        .setMembers(config.getClusterConfig().getMembers()
            .values()
            .stream()
            .filter(node -> node.getType() == Member.Type.PERSISTENT)
            .map(node -> node.getId().id())
            .collect(Collectors.toSet()))
        .setDataDirectory(String.format("%s/%s", DATA_PATH, GROUP_NAME)));
  }
}
