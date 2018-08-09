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

import com.google.common.collect.Sets;
import io.atomix.core.AtomixConfig;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.protocols.raft.partition.RaftStorageConfig;

import java.util.Collection;

/**
 * Consensus profile.
 */
public class ConsensusProfile implements Profile {
  public static final Type TYPE = new Type();

  /**
   * Creates a new consensus profile builder.
   *
   * @return a new consensus profile builder
   */
  public static ConsensusProfileBuilder builder() {
    return new ConsensusProfileBuilder();
  }

  /**
   * Consensus profile type.
   */
  public static class Type implements Profile.Type<ConsensusProfileConfig> {
    private static final String NAME = "consensus";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public ConsensusProfileConfig newConfig() {
      return new ConsensusProfileConfig();
    }

    @Override
    public Profile newProfile(ConsensusProfileConfig config) {
      return new ConsensusProfile(config);
    }
  }

  private final ConsensusProfileConfig config;

  ConsensusProfile(String... members) {
    this(Sets.newHashSet(members));
  }

  ConsensusProfile(Collection<String> members) {
    this(new ConsensusProfileConfig().setMembers(Sets.newHashSet(members)));
  }

  ConsensusProfile(ConsensusProfileConfig config) {
    this.config = config;
  }

  @Override
  public ConsensusProfileConfig config() {
    return config;
  }

  @Override
  public void configure(AtomixConfig config) {
    config.setManagementGroup(new RaftPartitionGroupConfig()
        .setName(this.config.getManagementGroup())
        .setPartitionSize(this.config.getMembers().size())
        .setPartitions(1)
        .setMembers(this.config.getMembers())
        .setStorageConfig(new RaftStorageConfig()
            .setDirectory(String.format("%s/%s", this.config.getDataPath(), this.config.getManagementGroup()))));
    config.addPartitionGroup(new RaftPartitionGroupConfig()
        .setName(this.config.getDataGroup())
        .setPartitionSize(this.config.getPartitionSize())
        .setPartitions(this.config.getPartitions())
        .setMembers(this.config.getMembers())
        .setStorageConfig(new RaftStorageConfig()
            .setDirectory(String.format("%s/%s", this.config.getDataPath(), this.config.getDataGroup()))));
  }
}
