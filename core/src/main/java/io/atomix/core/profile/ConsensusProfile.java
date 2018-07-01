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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Consensus profile type.
   */
  public static class Type implements Profile.Type<Config> {
    private static final String NAME = "consensus";

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
      return new ConsensusProfile(config);
    }
  }

  /**
   * Consensus profile builder.
   */
  public static class Builder implements Profile.Builder {
    private final Config config = new Config();

    private Builder() {
    }

    /**
     * Sets the consensus data file path.
     *
     * @param dataPath the consensus data file path
     * @return the consensus profile builder
     */
    public Builder setDataPath(String dataPath) {
      config.setDataPath(dataPath);
      return this;
    }

    /**
     * Sets the management partition group name.
     *
     * @param managementGroup the management partition group name
     * @return the consensus profile builder
     */
    public Builder setManagementGroup(String managementGroup) {
      config.setManagementGroup(managementGroup);
      return this;
    }

    /**
     * Sets the data partition group name.
     *
     * @param dataGroup the data partition group name
     * @return the consensus profile builder
     */
    public Builder setDataGroup(String dataGroup) {
      config.setDataGroup(dataGroup);
      return this;
    }

    /**
     * Sets the data partition size.
     *
     * @param partitionSize the data partition size
     * @return the consensus profile builder
     */
    public Builder setPartitionSize(int partitionSize) {
      config.setPartitionSize(partitionSize);
      return this;
    }

    /**
     * Sets the number of data partitions to configure.
     *
     * @param numPartitions the number of data partitions to configure
     * @return the consensus profile builder
     */
    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    /**
     * Sets the consensus members.
     *
     * @param members the consensus members
     * @return the profile builder
     */
    public Builder withMembers(Set<String> members) {
      config.setMembers(members);
      return this;
    }

    @Override
    public Profile build() {
      return new ConsensusProfile(config);
    }
  }

  /**
   * Consensus profile configuration.
   */
  public static class Config implements Profile.Config {
    private String dataPath = ".data";
    private String managementGroup = "system";
    private String dataGroup = "raft";
    private int partitionSize = 3;
    private int partitions = 7;
    private Set<String> members = Collections.emptySet();

    @Override
    public Profile.Type getType() {
      return TYPE;
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
    public Config setDataPath(String dataPath) {
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
     * @return the consensus profile configurations
     */
    public Config setDataGroup(String dataGroup) {
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
    public Config setPartitionSize(int partitionSize) {
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
    public Config setPartitions(int partitions) {
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
    public Config setMembers(Set<String> members) {
      this.members = members;
      return this;
    }
  }

  private final Config config;

  ConsensusProfile(String... members) {
    this(Sets.newHashSet(members));
  }

  ConsensusProfile(Collection<String> members) {
    this(new Config().setMembers(Sets.newHashSet(members)));
  }

  private ConsensusProfile(Config config) {
    this.config = config;
  }

  @Override
  public Profile.Config config() {
    return config;
  }

  @Override
  public void configure(AtomixConfig config) {
    config.setManagementGroup(new RaftPartitionGroupConfig()
        .setName(this.config.getManagementGroup())
        .setPartitionSize(this.config.getMembers().size())
        .setPartitions(1)
        .setMembers(this.config.getMembers())
        .setDataDirectory(String.format("%s/%s", this.config.getDataPath(), this.config.getManagementGroup())));
    config.addPartitionGroup(new RaftPartitionGroupConfig()
        .setName(this.config.getDataGroup())
        .setPartitionSize(this.config.getPartitionSize())
        .setPartitions(this.config.getPartitions())
        .setMembers(this.config.getMembers())
        .setDataDirectory(String.format("%s/%s", this.config.getDataPath(), this.config.getDataGroup())));
  }
}
